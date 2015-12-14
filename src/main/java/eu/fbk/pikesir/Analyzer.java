package eu.fbk.pikesir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.SKOS;
import org.tartarus.snowball.SnowballProgram;

import ixa.kaflib.KAFDocument;

import eu.fbk.pikesir.TermVector.Builder;
import eu.fbk.rdfpro.util.QuadModel;

public abstract class Analyzer {

    public abstract void analyze(KAFDocument document, QuadModel model,
            TermVector.Builder builder, boolean isQuery);

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    public static Analyzer concat(final Analyzer... analyzers) {
        if (analyzers.length == 0) {
            return createNullAnalyzer();
        } else if (analyzers.length == 1) {
            return analyzers[0];
        } else {
            return new ConcatAnalyzer(analyzers);
        }
    }

    public static Analyzer createNullAnalyzer() {
        return NullAnalyzer.INSTANCE;
    }

    public static Analyzer createStemAnalyzer(final String stemmerClass,
            @Nullable final Iterable<String> stopWords) {
        return new StemAnalyzer(stemmerClass, stopWords);
    }

    public static Analyzer createURIAnalyzer() {
        return URIAnalyzer.INSTANCE;
    }

    public static Analyzer createTypeAnalyzer() {
        return TypeAnalyzer.INSTANCE;
    }

    public static Analyzer createPredicateAnalyzer() {
        return PredicateAnalyzer.INSTANCE;
    }

    public static Analyzer createRoleAnalyzer() {
        return RoleAnalyzer.INSTANCE;
    }

    public static Analyzer createConceptAnalyzer() {
        return ConceptAnalyzer.INSTANCE;
    }

    public static Analyzer create(final Path root, final Properties properties, String prefix) {

        // Normalize prefix, ensuring it ends with '.'
        prefix = prefix.endsWith(".") ? prefix : prefix + ".";

        // Build a list of analyzers to be later combined
        final List<Analyzer> analyzers = new ArrayList<>();

        // Add an analyzer extracting stems, if enabled
        if (Boolean.parseBoolean(properties.getProperty(prefix + "stem", "false"))) {
            final String stemmerClass = properties.getProperty(prefix + "stemmer");
            final String stopwordsProp = properties.getProperty(prefix + "stopwords");
            final Set<String> stopwords = stopwordsProp == null ? null : ImmutableSet
                    .copyOf(stopwordsProp.split("\\s+"));
            analyzers.add(createStemAnalyzer(stemmerClass, stopwords));
        }

        // Add a URI analyzer, if enabled
        if (Boolean.parseBoolean(properties.getProperty(prefix + "uri", "false"))) {
            analyzers.add(createURIAnalyzer());
        }

        // Add a type analyzer, if enabled
        if (Boolean.parseBoolean(properties.getProperty(prefix + "type", "false"))) {
            analyzers.add(createTypeAnalyzer());
        }

        // Add a predicate analyzer, if enabled
        if (Boolean.parseBoolean(properties.getProperty(prefix + "predicate", "false"))) {
            analyzers.add(createPredicateAnalyzer());
        }

        // Add a role analyzer, if enabled
        if (Boolean.parseBoolean(properties.getProperty(prefix + "role", "false"))) {
            analyzers.add(createRoleAnalyzer());
        }

        // Add a concept analyzer, if enabled
        if (Boolean.parseBoolean(properties.getProperty(prefix + "concept", "false"))) {
            analyzers.add(createConceptAnalyzer());
        }

        // Combine the enrichers
        return concat(analyzers.toArray(new Analyzer[analyzers.size()]));
    }

    private static boolean isFrameBaseMicroframe(final URI uri) {
        if (!uri.getNamespace().equals("http://framebase.org/ns/")) {
            return false;
        }
        final String str = uri.getLocalName();
        final int index = str.lastIndexOf('.');
        if (index < 0) {
            return false;
        }
        for (int i = index + 1; i < str.length(); ++i) {
            final char ch = str.charAt(i);
            if (ch < 'a' || ch > 'z') {
                return false;
            }
        }
        return true;
    }

    private static final class ConcatAnalyzer extends Analyzer {

        private final Analyzer[] analyzers;

        ConcatAnalyzer(final Analyzer... analyzers) {
            this.analyzers = analyzers.clone();
        }

        @Override
        public void analyze(final KAFDocument document, final QuadModel model,
                final Builder builder, final boolean isQuery) {
            for (final Analyzer analyzer : this.analyzers) {
                analyzer.analyze(document, model, builder, isQuery);
            }
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" + Joiner.on(", ").join(this.analyzers) + ")";
        }

    }

    private static final class NullAnalyzer extends Analyzer {

        static final NullAnalyzer INSTANCE = new NullAnalyzer();

        @Override
        public void analyze(final KAFDocument document, final QuadModel model,
                final Builder builder, final boolean isQuery) {
        }

    }

    private static final class StemAnalyzer extends Analyzer {

        // Based on WordDelimiterFilter

        private static final String DEFAULT_STEMMER_TYPE = "EnglishStemmer";

        private static final Set<String> DEFAULT_STOP_WORDS = ImmutableSet.of("a", "an", "and",
                "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is", "it", "no",
                "not", "of", "on", "or", "such", "that", "the", "their", "then", "there", "these",
                "they", "this", "to", "was", "will", "with", "'s"); // from Lucene + 's

        private static final int LOWER = 0x01;

        private static final int UPPER = 0x02;

        private static final int DIGIT = 0x04;

        private static final int SUBWORD_DELIM = 0x08;

        private static final int ALPHA = LOWER | UPPER;

        private static final byte[] WORD_DELIM_TABLE;

        static {
            final byte[] tab = new byte[256];
            for (int i = 0; i < 256; i++) {
                byte code = 0;
                if (Character.isLowerCase(i)) {
                    code |= LOWER;
                } else if (Character.isUpperCase(i)) {
                    code |= UPPER;
                } else if (Character.isDigit(i)) {
                    code |= DIGIT;
                }
                if (code == 0) {
                    code = SUBWORD_DELIM;
                }
                tab[i] = code;
            }
            WORD_DELIM_TABLE = tab;
        }

        private final Class<? extends SnowballProgram> stemmerClass;

        private final Set<String> stopWords;

        @SuppressWarnings({ "unchecked", "rawtypes" })
        StemAnalyzer(@Nullable final String stemmerClass,
                @Nullable final Iterable<String> stopWords) {
            try {
                this.stemmerClass = (Class) Class.forName("org.tartarus.snowball.ext."
                        + (stemmerClass != null ? stemmerClass : DEFAULT_STEMMER_TYPE));
                this.stopWords = stopWords != null ? ImmutableSet.copyOf(stopWords)
                        : DEFAULT_STOP_WORDS;
            } catch (final ClassNotFoundException ex) {
                throw new IllegalArgumentException("Invalid stemmer " + stemmerClass);
            }
        }

        @Override
        public void analyze(final KAFDocument document, final QuadModel model,
                final Builder builder, final boolean isQuery) {

            // Iterate over all the tokens in the document
            for (final ixa.kaflib.Term term : document.getTerms()) {
                final String wf = term.getStr().trim();
                for (final String subWord : extract(wf)) {
                    if (isValidTerm(subWord)) {
                        try {
                            final SnowballProgram stemmer = this.stemmerClass.newInstance();
                            stemmer.setCurrent(subWord.toLowerCase());
                            stemmer.stem();
                            final String subWordStem = stemmer.getCurrent();
                            builder.addTerm(Field.STEM, subWordStem);
                        } catch (final InstantiationException | IllegalAccessException ex) {
                            Throwables.propagate(ex);
                        }
                    }
                }
            }
        }

        private boolean isValidTerm(final String wf) {
            if (wf.length() >= 2 && wf.length() <= 200
                    && !this.stopWords.contains(wf.toLowerCase())) {
                for (int i = 0; i < wf.length(); ++i) {
                    if (Character.isLetterOrDigit(wf.charAt(i))) {
                        return true;
                    }
                }
            }
            return false;
        }

        private static int charType(final int ch) {
            if (ch < WORD_DELIM_TABLE.length) {
                return WORD_DELIM_TABLE[ch];
            } else if (Character.isLowerCase(ch)) {
                return LOWER;
            } else if (Character.isLetter(ch)) {
                return UPPER;
            } else {
                return SUBWORD_DELIM;
            }
        }

        private static Set<String> extract(final String token) {
            final List<String> subTokens = Lists.newArrayList();
            final int len = token.length();
            if (len != 0) {
                int start = 0;
                int type = charType(token.charAt(start));
                while (start < len) {
                    while ((type & SUBWORD_DELIM) != 0 && ++start < len) {
                        type = charType(token.charAt(start));
                    }
                    int pos = start;
                    int lastType = type;
                    while (pos < len) {
                        if (type != lastType && ((lastType & UPPER) == 0 || (type & LOWER) == 0)) {
                            subTokens.add(token.substring(start, pos));
                            break;
                        }
                        if (++pos >= len) {
                            subTokens.add(token.substring(start, pos));
                            break;
                        }
                        lastType = type;
                        type = charType(token.charAt(pos));
                    }
                    start = pos;
                }
                final int numtok = subTokens.size();
                if (numtok > 1) {
                    subTokens.add(Joiner.on("").join(subTokens));
                    String tok = subTokens.get(0);
                    boolean isWord = (charType(tok.charAt(0)) & ALPHA) != 0;
                    boolean wasWord = isWord;
                    for (int i = 0; i < numtok;) {
                        int j;
                        for (j = i + 1; j < numtok; j++) {
                            wasWord = isWord;
                            tok = subTokens.get(j);
                            isWord = (charType(tok.charAt(0)) & ALPHA) != 0;
                            if (isWord != wasWord) {
                                break;
                            }
                        }
                        subTokens.add(Joiner.on("").join(subTokens.subList(i, j)));
                        i = j;
                    }
                }
            }
            subTokens.add(token);
            return ImmutableSet.copyOf(subTokens);
        }

    }

    private static final class URIAnalyzer extends Analyzer {

        static final URIAnalyzer INSTANCE = new URIAnalyzer();

        private static final URI ENTITY_CLASS = new URIImpl(
                "http://dkm.fbk.eu/ontologies/knowledgestore#Entity");

        @Override
        public void analyze(final KAFDocument document, final QuadModel model,
                final Builder builder, final boolean isQuery) {
            for (final Resource entity : model.filter(null, RDF.TYPE, ENTITY_CLASS).subjects()) {
                if (entity instanceof URI) {
                    final URI uri = (URI) entity;
                    if (uri.getNamespace().equals("http://dbpedia.org/resource/")) {
                        builder.addTerm(Field.URI, uri.getLocalName());
                    }
                }
            }
        }

    }

    private static final class TypeAnalyzer extends Analyzer {

        static final TypeAnalyzer INSTANCE = new TypeAnalyzer();

        private static final Map<String, Field> TYPE_MAP = ImmutableMap.of( //
                "http://dbpedia.org/class/yago/", Field.TYPE_YAGO, //
                "http://www.ontologyportal.org/SUMO.owl#", Field.TYPE_SUMO);

        @Override
        public void analyze(final KAFDocument document, final QuadModel model,
                final Builder builder, final boolean isQuery) {

            final Multimap<Field, Statement> stmts = HashMultimap.create();
            for (final Statement stmt : model.filter(null, RDF.TYPE, null)) {
                if (stmt.getObject() instanceof URI) {
                    final URI type = (URI) stmt.getObject();
                    final Field field = TYPE_MAP.get(type.getNamespace());
                    if (field != null) {
                        stmts.put(field, stmt);
                    }
                }
            }

            if (isQuery) {
                final Map<URI, List<URI>> parentMap = Maps.newHashMap();
                for (final Field field : stmts.keySet()) {
                    for (final Statement stmt : ImmutableList.copyOf(stmts.get(field))) {
                        final URI type = (URI) stmt.getObject();
                        List<URI> parentTypes = parentMap.get(type);
                        if (parentTypes == null) {
                            parentTypes = Lists.newArrayList();
                            parentMap.put(type, parentTypes);
                            for (final Value parentType : model
                                    .filter(type, RDFS.SUBCLASSOF, null).objects()) {
                                if (parentType instanceof URI && !parentType.equals(type)) {
                                    parentTypes.add((URI) parentType);
                                }
                            }
                        }
                        for (final URI parentType : parentTypes) {
                            stmts.remove(field, new StatementImpl(stmt.getSubject(), RDF.TYPE,
                                    parentType));
                        }
                    }
                }
            }

            for (final Field field : stmts.keySet()) {
                for (final Statement stmt : stmts.get(field)) {
                    builder.addTerm(field, ((URI) stmt.getObject()).getLocalName());
                }
            }
        }

    }

    private static final class PredicateAnalyzer extends Analyzer {

        static final PredicateAnalyzer INSTANCE = new PredicateAnalyzer();

        private static final Map<String, Field> PREDICATE_MAP = ImmutableMap.of( //
                "http://framebase.org/ns/", Field.PREDICATE_FRB, //
                "http://www.newsreader-project.eu/ontologies/propbank/", Field.PREDICATE_PB, //
                "http://www.newsreader-project.eu/ontologies/nombank/", Field.PREDICATE_NB);

        @Override
        public void analyze(final KAFDocument document, final QuadModel model,
                final Builder builder, final boolean isQuery) {

            final Multimap<Field, Statement> stmts = HashMultimap.create();
            for (final Statement stmt : model.filter(null, RDF.TYPE, null)) {
                if (stmt.getObject() instanceof URI) {
                    final URI type = (URI) stmt.getObject();
                    final Field field = PREDICATE_MAP.get(type.getNamespace());
                    if (field != null) {
                        stmts.put(field, stmt);
                    }
                }
            }

            if (isQuery) {
                final Map<URI, List<URI>> parentMap = Maps.newHashMap();
                for (final Field field : stmts.keySet()) {
                    for (final Statement stmt : ImmutableList.copyOf(stmts.get(field))) {
                        final URI type = (URI) stmt.getObject();
                        if (!isFrameBaseMicroframe(type)) {
                            List<URI> parentTypes = parentMap.get(type);
                            if (parentTypes == null) {
                                parentTypes = Lists.newArrayList();
                                parentMap.put(type, parentTypes);
                                for (final Value parentType : model.filter(type, RDFS.SUBCLASSOF,
                                        null).objects()) {
                                    if (parentType instanceof URI && !parentType.equals(type)) {
                                        parentTypes.add((URI) parentType);
                                    }
                                }
                            }
                            for (final URI parentType : parentTypes) {
                                stmts.remove(field, new StatementImpl(stmt.getSubject(), RDF.TYPE,
                                        parentType));
                            }
                        }
                    }
                }
            }

            for (final Field field : stmts.keySet()) {
                for (final Statement stmt : stmts.get(field)) {
                    builder.addTerm(field, ((URI) stmt.getObject()).getLocalName());
                }
            }
        }

    }

    private static final class RoleAnalyzer extends Analyzer {

        static final RoleAnalyzer INSTANCE = new RoleAnalyzer();

        private static final Map<String, Field> ROLE_MAP = ImmutableMap.of( //
                "http://framebase.org/ns/", Field.ROLE_FRB, //
                "http://www.newsreader-project.eu/ontologies/propbank/", Field.ROLE_PB, //
                "http://www.newsreader-project.eu/ontologies/nombank/", Field.ROLE_NB);

        @Override
        public void analyze(final KAFDocument document, final QuadModel model,
                final Builder builder, final boolean isQuery) {

            final Multimap<Field, Statement> stmts = HashMultimap.create();
            for (final Statement stmt : model) {
                final String ns = stmt.getPredicate().getNamespace();
                final Field field = ROLE_MAP.get(ns);
                if (field != null) {
                    stmts.put(field, stmt);
                }
            }

            if (isQuery) {
                final Map<URI, List<URI>> parentMap = Maps.newHashMap();
                for (final Field field : stmts.keySet()) {
                    for (final Statement stmt : ImmutableList.copyOf(stmts.get(field))) {
                        final URI pred = stmt.getPredicate();
                        List<URI> parentPreds = parentMap.get(pred);
                        if (parentPreds == null) {
                            parentPreds = Lists.newArrayList();
                            parentMap.put(pred, parentPreds);
                            for (final Value parentPred : model.filter(pred, RDFS.SUBPROPERTYOF,
                                    null).objects()) {
                                if (parentPred instanceof URI && !parentPred.equals(pred)) {
                                    parentPreds.add((URI) parentPred);
                                }
                            }
                        }
                        for (final URI parentPred : parentPreds) {
                            stmts.remove(field, new StatementImpl(stmt.getSubject(), parentPred,
                                    stmt.getObject()));
                        }
                    }
                }
            }

            for (final Field field : stmts.keySet()) {
                for (final Statement stmt : stmts.get(field)) {
                    builder.addTerm(field, stmt.getPredicate().getLocalName());
                }
            }
        }

    }

    private static final class ConceptAnalyzer extends Analyzer {

        static final ConceptAnalyzer INSTANCE = new ConceptAnalyzer();

        private static final Map<String, String> CONCEPT_MAP = ImmutableMap.of( //
                "http://dbpedia.org/class/yago/", "dbyago", //
                "http://framebase.org/ns/", "frb", //
                "http://dbpedia.org/resource/", "dbpedia", //
                "entity:", "entity");

        private static final URI ENTITY_CLASS = new URIImpl(
                "http://dkm.fbk.eu/ontologies/knowledgestore#Entity");

        @Override
        public void analyze(final KAFDocument document, final QuadModel model,
                final Builder builder, final boolean isQuery) {

            for (final Resource entity : model.filter(null, RDF.TYPE, ENTITY_CLASS).subjects()) {

                final Set<URI> concepts = Sets.newHashSet();
                final List<URI> queue = Lists.newLinkedList();

                for (final Value type : model.filter(entity, RDF.TYPE, null).objects()) {
                    if (type instanceof URI
                            && CONCEPT_MAP.containsKey(((URI) type).getNamespace())) {
                        concepts.add((URI) type);
                    }
                }
                for (final URI type : ImmutableList.copyOf(concepts)) {
                    if (!isFrameBaseMicroframe(type)) {
                        final Set<Value> parents = Sets.newHashSet(model.filter(type,
                                RDFS.SUBCLASSOF, null).objects());
                        parents.remove(type);
                        concepts.removeAll(parents);
                    }
                }

                if (entity instanceof URI
                        && ((URI) entity).getNamespace().equals("http://dbpedia.org/resource/")) {
                    concepts.add((URI) entity);
                }

                if (!isQuery) {
                    queue.addAll(concepts);
                    while (!queue.isEmpty()) {
                        final URI uri = queue.remove(0);
                        for (final Value parent : model.filter(uri, SKOS.BROADER, null).objects()) {
                            if (parent instanceof URI) {
                                final URI parentURI = (URI) parent;
                                if (CONCEPT_MAP.containsKey(parentURI.getNamespace())
                                        && !concepts.contains(parentURI)) {
                                    concepts.add(parentURI);
                                    queue.add(parentURI);
                                }
                            }
                        }
                    }
                }

                for (final URI concept : concepts) {
                    final String prefix = CONCEPT_MAP.get(concept.getNamespace());
                    final String name = prefix + ":" + concept.getLocalName();
                    builder.addTerm(Field.CONCEPT, name);
                }
            }
        }

    }

}
