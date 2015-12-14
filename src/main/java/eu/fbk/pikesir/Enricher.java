package eu.fbk.pikesir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.SESAME;
import org.openrdf.rio.RDFHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.pikesir.util.KeyQuadIndex;
import eu.fbk.pikesir.util.KeyQuadSource;
import eu.fbk.rdfpro.RDFHandlers;
import eu.fbk.rdfpro.RDFProcessors;
import eu.fbk.rdfpro.RDFSources;
import eu.fbk.rdfpro.util.QuadModel;

public abstract class Enricher {

    private static final Logger LOGGER = LoggerFactory.getLogger(Enricher.class);

    public abstract void enrich(QuadModel model);

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    public static Enricher concat(final Enricher... enrichers) {
        if (enrichers.length == 0) {
            return createNullEnricher();
        } else if (enrichers.length == 1) {
            return enrichers[0];
        } else {
            return new ConcatEnricher(enrichers);
        }
    }

    public static Enricher createNullEnricher() {
        return NullEnricher.INSTANCE;
    }

    public static Enricher createRDFSEnricher() {
        return RDFSEnricher.INSTANCE;
    }

    public static Enricher createURIEnricher(final Path indexPath,
            final Iterable<String> nonRecursiveNamespaces,
            final Iterable<String> recursiveNamespaces) {
        return new URIEnricher(indexPath, nonRecursiveNamespaces, recursiveNamespaces);
    }

    public static Enricher create(final Path root, final Properties properties, String prefix) {

        // Normalize prefix, ensuring it ends with '.'
        prefix = prefix.endsWith(".") ? prefix : prefix + ".";

        // Build a list of enrichers to be later combined
        final List<Enricher> enrichers = new ArrayList<>();

        // Add an enricher adding triples about certain URIs, possibly recursively
        final String uriIndexPath = properties.getProperty(prefix + "uri.index");
        if (uriIndexPath != null) {
            final Set<String> recursionNS = ImmutableSet.copyOf(properties.getProperty(
                    prefix + "uri.recursion", "").split("\\s+"));
            final Set<String> noRecursionNS = ImmutableSet.copyOf(properties.getProperty(
                    prefix + "uri.norecursion", "").split("\\s+"));
            if (!recursionNS.isEmpty() && !noRecursionNS.isEmpty()) {
                enrichers.add(createURIEnricher(root.resolve(uriIndexPath), noRecursionNS,
                        recursionNS));
            }
        }

        // Add an enricher computing the RDFS closure of the model, if enabled
        if (Boolean.parseBoolean(properties.getProperty(prefix + "rdfs", "false"))) {
            enrichers.add(createRDFSEnricher());
        }

        // Combine the enrichers
        return concat(enrichers.toArray(new Enricher[enrichers.size()]));
    }

    private static final class ConcatEnricher extends Enricher {

        private final Enricher[] enrichers;

        ConcatEnricher(final Enricher... enrichers) {
            this.enrichers = enrichers.clone();
        }

        @Override
        public void enrich(final QuadModel model) {
            for (final Enricher enricher : this.enrichers) {
                enricher.enrich(model);
            }
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" + Joiner.on(", ").join(this.enrichers) + ")";
        }

    }

    private static final class NullEnricher extends Enricher {

        static final NullEnricher INSTANCE = new NullEnricher();

        @Override
        public void enrich(final QuadModel model) {
        }

    }

    private static final class RDFSEnricher extends Enricher {

        static final RDFSEnricher INSTANCE = new RDFSEnricher();

        @Override
        public void enrich(final QuadModel model) {
            try {
                final int numTriplesBefore = model.size();
                RDFProcessors.rdfs(RDFSources.wrap(ImmutableList.copyOf(model)), SESAME.NIL, true,
                        true, "rdfs4a", "rdfs4b", "rdfs8").apply(RDFSources.NIL,
                        RDFHandlers.wrap(model), 1);
                LOGGER.debug("Inferred {} triples (total {})", model.size() - numTriplesBefore,
                        model.size());
            } catch (final RDFHandlerException ex) {
                Throwables.propagate(ex);
            }
        }

    }

    private static final class URIEnricher extends Enricher {

        private final Path indexPath;

        @Nullable
        private KeyQuadSource index;

        private final Set<String> nonRecursiveNamespaces;

        private final Set<String> recursiveNamespaces;

        URIEnricher(final Path indexPath, final Iterable<String> nonRecursiveNamespaces,
                final Iterable<String> recursiveNamespaces) {

            this.indexPath = Objects.requireNonNull(indexPath);
            this.index = null;
            this.nonRecursiveNamespaces = nonRecursiveNamespaces == null ? ImmutableSet.of()
                    : ImmutableSet.copyOf(nonRecursiveNamespaces);
            this.recursiveNamespaces = recursiveNamespaces == null ? ImmutableSet.of()
                    : ImmutableSet.copyOf(recursiveNamespaces);
        }

        @Override
        public void enrich(final QuadModel model) {

            final Set<URI> uris = Sets.newHashSet();
            for (final Statement stmt : model) {
                collect(uris, stmt.getSubject());
                collect(uris, stmt.getPredicate());
                collect(uris, stmt.getObject());
            }

            try {
                final int numTriplesBefore = model.size();
                getIndex().getRecursive(uris, (final Value v) -> {
                    return matches(v, this.recursiveNamespaces);
                }, RDFHandlers.wrap(model));
                LOGGER.debug("Enriched {} URIs with {} triples", uris.size(), model.size()
                        - numTriplesBefore);
            } catch (final RDFHandlerException ex) {
                Throwables.propagate(ex);
            }
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(path: " + this.indexPath + ", recursion:"
                    + this.recursiveNamespaces + ". norecursion:" + this.nonRecursiveNamespaces
                    + ")";
        }

        private void collect(final Set<URI> set, final Value value) {
            if (value instanceof URI
                    && (matches(value, this.nonRecursiveNamespaces) || matches(value,
                            this.recursiveNamespaces))) {
                set.add((URI) value);
            }
        }

        private boolean matches(final Value value, final Set<String> namespaces) {
            if (value instanceof URI) {
                final String str = value.stringValue();
                for (final String namespace : namespaces) {
                    if (str.startsWith(namespace)) {
                        return true;
                    }
                }
            }
            return false;
        }

        private synchronized KeyQuadSource getIndex() {
            if (this.index == null) {
                this.index = new KeyQuadIndex(this.indexPath.toFile());
            }
            return this.index;
        }

    }

}
