package eu.fbk.pikesir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
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
import eu.fbk.rdfpro.RDFHandlers;
import eu.fbk.rdfpro.RDFProcessors;
import eu.fbk.rdfpro.RDFSources;
import eu.fbk.rdfpro.util.QuadModel;

/**
 * Enriches a knowledge graph extracted from a document or query before it is analyzed for
 * extracting semantic terms.
 *
 * <p>
 * Enrichment is performed by calling method {@link #enrich(QuadModel)} and consists in adding
 * additional triples to the graph. Two basic kinds of enrichment are provided by this class
 * (additional enrichers can be implemented by subclassing):
 * <ul>
 * <li>URI enrichment (configured via {@link #createURIEnricher(Path, Iterable, Iterable)} matches
 * selected URIs in the graph and add additional triples having them as subjects, possibly
 * repeating the process recursively for newly introduced URIs;</li>
 * <li>RDFS enrichment (configured via {@link #createRDFSEnricher()} materializes the RDFS closure
 * of the knowledge graph.</li>
 * </ul>
 * These enrichers can be concatenated in a composite enricher applying them in sequence via
 * method {@link #concat(Enricher...)}. Method {@link #create(Path, Properties, String)} allows to
 * create an enricher based on a {@link Properties} object.
 * </p>
 */
public abstract class Enricher {

    private static final Logger LOGGER = LoggerFactory.getLogger(Enricher.class);

    /**
     * Enriches the (document or query) knowledge graph specified. The graph is passed as
     * QuadModel which is modified in place by the method.
     *
     * @param model
     *            the graph to enrich
     */
    public abstract void enrich(QuadModel model);

    /**
     * {@inheritDoc} Emits a descriptive string describe the Enricher and its configuration. This
     * implementation emits the class name.
     */
    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    /**
     * Returns a composite enricher that concatenates the supplied enricher, calling each of them
     * in sequence on the same knowledge graph.
     *
     * @param enrichers
     *            the enrichers to concatenate
     * @return the resulting composite enricher
     */
    public static Enricher concat(final Enricher... enrichers) {
        if (enrichers.length == 0) {
            return createNullEnricher();
        } else if (enrichers.length == 1) {
            return enrichers[0];
        } else {
            return new ConcatEnricher(enrichers);
        }
    }

    /**
     * Returns a null enricher that does not perform any form of enrichment.
     *
     * @return a null enricher, which does not modify the supplied knowledge graph.
     */
    public static Enricher createNullEnricher() {
        return NullEnricher.INSTANCE;
    }

    /**
     * Returns an enricher that adds to the knowledge graph all the triples that can be
     * materialized via RDFS inference. Inference is performed via RDFpro. {@code rdfs:Resource}
     * type triples are not materialized (i.e., rules rdfs4a, rdfs4b, rdfs8 are disabled).
     *
     * @return an enricher performing RDFS inference
     */
    public static Enricher createRDFSEnricher() {
        return RDFSEnricher.INSTANCE;
    }

    /**
     * Returns an enricher that augments selected URIs with additional triples loaded from an
     * external key-value index. The index is maps a URI to the set of RDF triples. If the
     * knowledge graph being enriched contains that URI, it is augmented with the associated set
     * of triples. The URIs to enrich are selected based on their namespaces. This process can be
     * done non-recursively, i.e., only for URIs originally present in the knowledge graph, or
     * recursively, i.e., also for URIs that are added to the knowledge graph as a result of a
     * previous enrichment. Recursion can be enabled selectively based on the URI namespace, and
     * is particularly useful to add TBox data, as it allows to add {@code rdfs:subClassOf}
     * triples for classes in the graph, and recursively all their superclasses and so on.
     *
     * @param indexPath
     *            the path where the files of the persistent key-value index are stored; the index
     *            must have been created in advance using {@link KeyQuadIndex#main(String...)}.
     * @param nonRecursiveNamespaces
     *            the URI namespaces for which to enable non-recursive enrichment
     * @param recursiveNamespaces
     *            the URI namespaces for which to enable recursive enrichment
     * @return the created enricher
     */
    public static Enricher createURIEnricher(final Path indexPath,
            final Iterable<String> nonRecursiveNamespaces,
            final Iterable<String> recursiveNamespaces) {
        return new URIEnricher(indexPath, nonRecursiveNamespaces, recursiveNamespaces);
    }

    /**
     * Returns an enricher based on the configuration properties supplied. The method looks for
     * certain properties within the {@link Properties} object supplied, prepending them an
     * optional prefix (e.g., if property is X and the prefix is Y, the method will look for
     * property Y.X). The path parameter is used as the base directory for resolving relative
     * paths contained in the examined properties. The properties currently supported are:
     * <ul>
     * <li>{@code type} - a space-separated list of types of {@code Enricher} to configure;
     * supported values are {@code uri} for {@link #createURIEnricher(Path, Iterable, Iterable)}
     * and {@code rdfs} for {@link #createRDFSEnricher()} (if both are specified, they are
     * concatenated in a single {@code Enricher});</li>
     * <li>{@code uri.index} - if specified, a URI enricher is configured loading the associated
     * index from the path used as value of the property;</li>
     * <li>{@code uri.recursion} - a space-separated list of namespace URI strings controlling for
     * which URIs recursive URI enrichment should be enabled;</li>
     * <li>{@code uri.norecursion} - a space-separated list of namespace URI strings controlling
     * for which URIs non-recursive URI enrichment should be enabled;</li>
     * </ul>
     *
     * @param root
     *            the base directory for resolving relative paths
     * @param properties
     *            the configuration properties
     * @param prefix
     *            an optional prefix to prepend to supported properties
     * @return an {@code Enricher} based on the specified configuration (if successful)
     */
    public static Enricher create(final Path root, final Properties properties,
            @Nullable String prefix) {

        // Normalize prefix, ensuring it ends with '.'
        prefix = Strings.isNullOrEmpty(prefix) ? "" : prefix.endsWith(".") ? prefix : prefix + ".";

        // Build a list of enrichers to be later combined
        final List<Enricher> enrichers = new ArrayList<>();

        // Retrieve the types of analyzer enabled in the configuration
        final Set<String> types = ImmutableSet.copyOf(properties.getProperty(prefix + "type", "")
                .split("\\s+"));

        // Add an enricher adding triples about certain URIs, possibly recursively
        if (types.contains("uri")) {
            final String uriIndexPath = properties.getProperty(prefix + "uri.index");
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
        if (types.contains("rdfs")) {
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
            // leave the knowledge graph unchanged
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
        private KeyQuadIndex index;

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

        private synchronized KeyQuadIndex getIndex() {
            if (this.index == null) {
                // The index is loaded on-demand, to avoid incurring in the associated cost when
                // the enricher object is created (e.g., because configured) but never called
                this.index = new KeyQuadIndex(this.indexPath.toFile());
            }
            return this.index;
        }

    }

}
