package eu.fbk.ke4ir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import javax.annotation.Nullable;
import javax.xml.datatype.XMLGregorianCalendar;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.XMLSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tartarus.snowball.SnowballProgram;

import eu.fbk.ke4ir.TermVector.Builder;
import eu.fbk.rdfpro.util.Hash;
import eu.fbk.rdfpro.util.Namespaces;
import eu.fbk.rdfpro.util.QuadModel;

import ixa.kaflib.KAFDocument;

/**
 * Analyzes a text (document or query) and the associated (enriched) knowledge graph, extracting
 * textual and semantic terms from them.
 */
public abstract class Analyzer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Analyzer.class);

    /**
     * Analyzes a document (or query, with their NLP annotations) and the associated knowledge
     * graph, extracting and emitting terms to the supplied {@code TermVector.Builder}.
     *
     * @param document
     *            a KAFDocument object containing the text and NLP annotations of the document or
     *            query to analyze
     * @param model
     *            the (enriched) knowledge graph associated to the document or query
     * @param builder
     *            the sink object where to send extracted terms
     * @param fromSentence
     *            the (1-based) index of the first sentence to process, included
     * @param toSentence
     *            the (1-based) index of the last sentence to process, included
     * @param layerPrefix
     *            the prefix to add to the layer of generated terms, not null
     */
    public abstract void analyze(KAFDocument document, QuadModel model, TermVector.Builder builder,
            int fromSentence, int toSentence, String layerPrefix);

    /**
     * {@inheritDoc} Emits a descriptive string describe the Analyzer and its configuration. This
     * implementation emits the class name.
     */
    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    /**
     * Returns a composite {@code Analyzer} that the specified analyzers to the input text and
     * knowledge graph, returning the concatenation of the terms extracted from each of them.
     *
     * @param analyzers
     *            the {@code Analyzer}s to concatenate
     * @return the resulting composite {@code Analyzer}
     */
    public static Analyzer concat(final Analyzer... analyzers) {
        if (analyzers.length == 0) {
            return createNullAnalyzer();
        } else if (analyzers.length == 1) {
            return analyzers[0];
        } else {
            return new ConcatAnalyzer(analyzers);
        }
    }

    /**
     * Returns a null {@code Analyzer} that does not extract any term from the supplied text and
     * knowledge graph.
     *
     * @return a null {@code Analyzer}, which does not extract any term
     */
    public static Analyzer createNullAnalyzer() {
        return NullAnalyzer.INSTANCE;
    }

    /**
     * Returns an {@code Analyzer} that extracts textual terms from the supplied text. The
     * knowledge graph is ignored. The returned {@code Analyzer} uses the same tokenization
     * produced by the NLP analysis of the text (i.e., the one included in the supplied
     * {@link KAFDocument} object). Tokens corresponding to compound words are emitted as is but
     * also a normalized version (spaces and other separators are removed) and their subwords are
     * emitted. All emitted terms are put in lowercase, stemmed (using the specified SnowBall
     * stemmer class) and stop words from the supplied list are removed. This pipeline is based on
     * and produces results very close to the default text processing of Lucene.
     *
     * @param stemmerClass
     *            the simple name of the stemmer class, to be chosen within package
     *            {@code org.tartarus.snowball.ext}; example: {@code EnglishStemmer}
     * @param stopWords
     *            a collection of stop words; example: {@code a, an, and, are, as, at, be,
     *            but, by, for, if, in, into, is, it, no, not, of, on, or, such, that, the,
     *            their, then, there, these, they, this, to, was, will, with, 's} (this is the
     *            default list used in Lucene for English texts)
     * @return the created textual {@code Analyzer}
     */
    public static Analyzer createTextualAnalyzer(final String stemmerClass,
            @Nullable final Iterable<String> stopWords) {
        return new TextualAnalyzer(stemmerClass, stopWords);
    }

    /**
     * Returns an {@code Analyzer} that extracts semantic terms from the supplied knowledge graph.
     * The text is ignored. Using the specified {@code denotedByProperty}, the returned
     * {@code Analyzer} identifies all the mentions in the text and the entities each of them
     * denotes. For each mention, it extracts four sets of URI, TYPE, FRAME and TIME terms. URI
     * terms are the URIs of denoted entities and their extraction is controlled by
     * {@code uriNamespaces}. TYPE terms are the types associated to denoted entities, controlled
     * by {@code typeNamespaces}. FRAME terms are {@code <frame type, participant URI>} pairs
     * where the frame type is controlled by {@code frameNamespaces}, and the participant URI must
     * correspond to a mentioned entity, linked to the frame entity, whose URI is matched by
     * {@code uriNamespaces}. TIME terms are finally extracted based on temporal information
     * (date/time literals, OWL time values) attached to denoted entities or their types. Each
     * term extracted from a mention is emitted with frequency 1 and weight equal to the
     * reciprocal of the number of terms extracted from that mention for that layer.
     *
     * @param denotedByProperty
     *            the URI of the property linking entities to mentions denoting them (exactly in
     *            that direction)
     * @param uriNamespaces
     *            the URI namespaces that denoted entities must have in order to generate URI
     *            terms; leave empty or null to disable the extraction of URI terms
     * @param typeNamespaces
     *            the URI namespaces that entity types must have in order to generate TYPE terms;
     *            leave empty or null to disable the extraction of TYPE terms
     * @param frameNamespaces
     *            the URI namespaces that an entity types must have in order for that entity to be
     *            recognized as a frame, leading to the emission of FRAME terms; leave empty or
     *            null to disable the extraction of FRAME terms
     * @return the created semantic {@code Analyzer}
     */
    public static Analyzer createSemanticAnalyzer(final URI denotedByProperty,
            final Iterable<String> uriNamespaces, final Iterable<String> typeNamespaces,
            final Iterable<String> frameNamespaces) {
        return new SemanticAnalyzer(denotedByProperty, uriNamespaces, typeNamespaces,
                frameNamespaces);
    }

    /**
     * Returns an {@code Analyzer} based on the configuration properties supplied. The method
     * looks for certain properties within the {@link Properties} object supplied, prepending them
     * an optional prefix (e.g., if property is X and the prefix is Y, the method will look for
     * property Y.X). The path parameter is used as the base directory for resolving relative
     * paths contained in the examined properties. The properties currently supported are:
     * <ul>
     * <li>{@code type} - a space-separated list of types of {@code Analyzer} to configure;
     * supported values are {@code textual} for {@link #createTextualAnalyzer(String, Iterable)}
     * and {@code semantic} for {@link #createSemanticAnalyzer(URI, Iterable, Iterable, Iterable)}
     * (if both are specified, they are concatenated in a single {@code Analyzer});</li>
     * <li>{@code textual.stemmer} - the name of the SnowBall stemmer class used by the textual
     * {@code Analyzer} (e.g., EnglishStemmer);</li>
     * <li>{@code textual.stopwords} - a space-separated list of stop word, used by the textual
     * {@code Analyzer};</li>
     * <li>{@code semantic.denotedBy} - the URI string of the property linking entities to
     * mentions denoting them, for use by the semantic {@code Analyzer};</li>
     * <li>{@code semantic.uri} - a space-separated list of URI namespaces controlling the
     * emission of URI terms by the semantic {@code Analyzer};</li>
     * <li>{@code semantic.type} - a space-separated list of URI namespaces controlling the
     * emission of TYPE terms by the semantic {@code Analyzer};</li>
     * <li>{@code semantic.frame} - a space-separated list of URI namespaces controlling the
     * emission of FRAME terms by the semantic {@code Analyzer}.</li>
     * </ul>
     *
     * @param root
     *            the base directory for resolving relative paths
     * @param properties
     *            the configuration properties
     * @param prefix
     *            an optional prefix to prepend to supported properties
     * @return an {@code Analyzer} based on the specified configuration (if successful)
     */
    public static Analyzer create(final Path root, final Properties properties, String prefix) {

        // Normalize prefix, ensuring it ends with '.'
        prefix = prefix.endsWith(".") ? prefix : prefix + ".";

        // Build a list of analyzers to be later combined
        final List<Analyzer> analyzers = new ArrayList<>();

        // Retrieve the types of analyzer enabled in the configuration
        final Set<String> types = ImmutableSet
                .copyOf(properties.getProperty(prefix + "type", "").split("\\s+"));

        // Add an analyzer extracting stems, if enabled
        if (types.contains("textual")) {
            final String stemmerClass = properties.getProperty(prefix + "textual.stemmer");
            final String stopwordsProp = properties.getProperty(prefix + "textual.stopwords");
            final Set<String> stopwords = stopwordsProp == null ? null
                    : ImmutableSet.copyOf(stopwordsProp.split("\\s+"));
            analyzers.add(createTextualAnalyzer(stemmerClass, stopwords));
        }

        // Add a semantic analyzer, if enabled
        if (types.contains("semantic")) {
            final URI denotedByProperty = new URIImpl(
                    properties.getProperty(prefix + "semantic.denotedby"));
            final Set<String> uriNamespaces = ImmutableSet
                    .copyOf(properties.getProperty(prefix + "semantic.uri", "").split("\\s+"));
            final Set<String> typeNamespaces = ImmutableSet
                    .copyOf(properties.getProperty(prefix + "semantic.type", "").split("\\s+"));
            final Set<String> frameNamespaces = ImmutableSet
                    .copyOf(properties.getProperty(prefix + "semantic.frame", "").split("\\s+"));
            analyzers.add(createSemanticAnalyzer(denotedByProperty, uriNamespaces, typeNamespaces,
                    frameNamespaces));
        }

        // Combine the analyzers (if necessary)
        return concat(analyzers.toArray(new Analyzer[analyzers.size()]));
    }

    private static final class ConcatAnalyzer extends Analyzer {

        private final Analyzer[] analyzers;

        ConcatAnalyzer(final Analyzer... analyzers) {
            this.analyzers = analyzers.clone();
        }

        @Override
        public void analyze(final KAFDocument document, final QuadModel model,
                final Builder builder, final int fromSentence, final int toSentence,
                final String layerPrefix) {
            for (final Analyzer analyzer : this.analyzers) {
                analyzer.analyze(document, model, builder, fromSentence, toSentence, layerPrefix);
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
                final Builder builder, final int fromSentence, final int toSentence,
                final String layerPrefix) {
        }

    }

    private static final class TextualAnalyzer extends Analyzer {

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
        TextualAnalyzer(@Nullable final String stemmerClass,
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
                final Builder builder, final int fromSentence, final int toSentence,
                final String layerPrefix) {

            // Iterate over all the tokens in the document
            for (int sentence = fromSentence; sentence <= toSentence; ++sentence) {
                final List<ixa.kaflib.Term> terms = document.getTermsBySent(sentence);
                if (terms == null || terms.isEmpty()) {
                    break;
                }
                for (final ixa.kaflib.Term term : terms) {
                    final String wf = term.getStr().trim();
                    for (final String subWord : extract(wf)) {
                        if (isValidTerm(subWord)) {
                            try {
                                final SnowballProgram stemmer = this.stemmerClass.newInstance();
                                stemmer.setCurrent(subWord.toLowerCase());
                                stemmer.stem();
                                final String subWordStem = stemmer.getCurrent();
                                builder.addTerm(layerPrefix + "textual", subWordStem);
                            } catch (final InstantiationException | IllegalAccessException ex) {
                                Throwables.propagate(ex);
                            }
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

    private static final class SemanticAnalyzer extends Analyzer {

        private static final URI OWLTIME_HAS_DATE_TIME_DESCRIPTION = new URIImpl(
                "http://www.w3.org/TR/owl-time#hasDateTimeDescription");

        private static final URI OWLTIME_YEAR = new URIImpl("http://www.w3.org/TR/owl-time#year");

        private static final URI OWLTIME_MONTH = new URIImpl(
                "http://www.w3.org/TR/owl-time#month");

        private static final URI OWLTIME_DAY = new URIImpl("http://www.w3.org/TR/owl-time#day");

        private static final URI NIF_BEGIN_INDEX = new URIImpl(
                "http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#beginIndex");

        private static final URI NIF_END_INDEX = new URIImpl(
                "http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#endIndex");

        private final URI denotedByProperty;

        private final Set<String> uriNamespaces;

        private final Set<String> typeNamespaces;

        private final Set<String> frameNamespaces;

        SemanticAnalyzer(final URI denotedByProperty,
                @Nullable final Iterable<String> uriNamespaces,
                @Nullable final Iterable<String> typeNamespaces,
                @Nullable final Iterable<String> frameNamespaces) {

            this.denotedByProperty = Objects.requireNonNull(denotedByProperty);
            this.uriNamespaces = uriNamespaces == null ? ImmutableSet.of()
                    : ImmutableSet.copyOf(uriNamespaces);
            this.typeNamespaces = typeNamespaces == null ? ImmutableSet.of()
                    : ImmutableSet.copyOf(typeNamespaces);
            this.frameNamespaces = frameNamespaces == null ? ImmutableSet.of()
                    : ImmutableSet.copyOf(frameNamespaces);
        }

        @Override
        public void analyze(final KAFDocument document, final QuadModel model,
                final Builder builder, final int fromSentence, final int toSentence,
                final String layerPrefix) {

            // Retrieve start and end offsets identifying the fragment of text to analyze
            final int fromOffset = extractSentenceFromOffset(document, fromSentence);
            final int toOffset = extractSentenceToOffset(document, toSentence);

            // Extract entities and mentions, plus the mapping mention -> denoted entities
            final Set<Resource> entities = Sets.newHashSet();
            final Multimap<Resource, Resource> mentions = HashMultimap.create();
            for (final Statement stmt : model.filter(null, this.denotedByProperty, null)) {
                if (stmt.getObject() instanceof URI) {
                    final URI mention = (URI) stmt.getObject();
                    if (containsMention(model, fromOffset, toOffset, mention)) {
                        entities.add(stmt.getSubject());
                        mentions.put(mention, stmt.getSubject());
                    }
                }
            }

            // Iterate over each mention with its denoted entities, emitting semantic terms
            for (final Collection<Resource> mentionEntities : mentions.asMap().values()) {

                // Allocate sets for the URI, TYPE, FRAME and TIME terms denoted by the mention
                final Set<String> uris = Sets.newHashSet();
                final Set<String> types = Sets.newHashSet();
                final Set<String> frames = Sets.newHashSet();
                final Set<String> times = Sets.newHashSet();

                // Populate the sets by iterating over the entities denoted by the current mention
                for (final Resource entity : mentionEntities) {

                    // Extract a URI term if the mentioned entity is a URI in a specific namespace
                    if (entity instanceof URI
                            && this.uriNamespaces.contains(((URI) entity).getNamespace())) {
                        uris.add(format((URI) entity));
                    }

                    // Extract TYPE and FRAME terms based on the types associated to the entity
                    for (final Value value : model.filter(entity, RDF.TYPE, null).objects()) {
                        if (!(value instanceof URI)) {
                            continue; // consider only URI types
                        }
                        final URI uri = (URI) value;
                        final String ns = uri.getNamespace();
                        if (this.typeNamespaces.contains(ns)) {
                            types.add(format(uri));
                        }
                        if (this.frameNamespaces.contains(ns)) {
                            // In case of frames we consider as participants all the entities
                            // connected to the frame entity that (1) have mentions in the document
                            // and (2) are identified by URIs in uriNamespaces
                            for (final Value part : model.filter(entity, null, null).objects()) {
                                if (part instanceof URI && entities.contains(part) && //
                                        this.uriNamespaces.contains(((URI) part).getNamespace())) {
                                    frames.add(format(uri) + "__" + format((URI) part));
                                }
                            }
                        }
                    }

                    // Extract TIME terms from the entity
                    for (final Statement stmt : model.filter(entity, null, null)) {
                        if (stmt.getObject() instanceof Literal) {
                            extractTimeComponents(times, (Literal) stmt.getObject());
                        } else if (stmt.getObject() instanceof Resource
                                && stmt.getPredicate().equals(OWLTIME_HAS_DATE_TIME_DESCRIPTION)) {
                            extractTimeComponents(times, (Resource) stmt.getObject(), model);
                        }
                    }
                }

                // Emit terms for each layer (for each term: frequency=1, weight=1/#terms in layer)
                emitTerms(builder, "uri", uris, layerPrefix);
                emitTerms(builder, "type", types, layerPrefix);
                emitTerms(builder, "frame", frames, layerPrefix);
                emitTerms(builder, "time", times, layerPrefix);
            }
        }

        private static String format(final URI uri) {
            final String ns = uri.getNamespace();
            String prefix = Namespaces.DEFAULT.prefixFor(ns);
            if (prefix == null) {
                // Generate a prefix using a hash of the namespace. A prefix is mandatory as we
                // had problems with Lucene if we fed it with <...> terms.
                prefix = Hash.murmur3(ns).toString();
            }
            return prefix + ":" + uri.getLocalName();
        }

        private static void emitTerms(final TermVector.Builder sink, final String termLayer,
                final Collection<String> termValues, final String layerPrefix) {
            final double weight = 1.0 / termValues.size();
            for (final String termValue : termValues) {
                sink.addTerm(layerPrefix + termLayer, termValue, 1, weight);
            }
        }

        private static void extractTimeComponents(final Collection<String> sink,
                final Literal literal) {

            // Determine which date components are available in the literal value
            final URI dt = literal.getDatatype();
            final boolean hasTime = dt.equals(XMLSchema.DATETIME);
            final boolean hasDay = hasTime || dt.equals(XMLSchema.DATE);
            final boolean hasMonth = hasDay || dt.equals(XMLSchema.GYEARMONTH);
            final boolean hasYear = hasMonth || dt.equals(XMLSchema.GYEAR);

            // Abort if there is no year component (=> there are no other components)
            if (!hasYear) {
                return;
            }

            try {
                // Strategy 1: rely on Sesame to parse the datetime value
                final XMLGregorianCalendar value = literal.calendarValue();
                final int year = value.getYear();
                sink.add("century:" + year / 100);
                sink.add("decade:" + year / 10);
                sink.add("year:" + year);
                if (hasMonth) {
                    final int month = value.getMonth();
                    sink.add("month:" + year + "-" + month);
                    if (hasDay) {
                        final int day = value.getDay();
                        sink.add("day:" + year + "-" + month + "-" + day);
                    }
                }

            } catch (final Throwable ex) {
                try {
                    // Strategy 2: assume format "YYYY-MM-DD"
                    final String label = literal.getLabel();
                    final Integer year = Integer.parseInt(label.substring(0, 4));
                    sink.add("century:" + year / 100);
                    sink.add("decade:" + year / 10);
                    sink.add("year:" + year);
                    final Integer month = Integer.parseInt(label.substring(5, 7));
                    sink.add("month:" + year + "-" + month);
                    final Integer day = Integer.parseInt(label.substring(8, 10));
                    sink.add("day:" + year + "-" + month + "-" + day);
                } catch (final Throwable ex2) {
                    ex2.addSuppressed(ex);
                    LOGGER.warn("Could not extract date components from literal " + literal, ex2);
                }
            }
        }

        private static void extractTimeComponents(final Collection<String> sink,
                final Resource owltimeDesc, final QuadModel model) {

            try {
                // Lookup year, month and day, in this order, and generate corresponding terms
                final Literal yearLit = model.filter(owltimeDesc, OWLTIME_YEAR, null)
                        .objectLiteral();
                if (yearLit != null) {
                    final int year = yearLit.intValue();
                    sink.add("century:" + year / 100);
                    sink.add("decade:" + year / 10);
                    sink.add("year:" + year);
                    final Literal monthLit = model.filter(owltimeDesc, OWLTIME_MONTH, null)
                            .objectLiteral();
                    if (monthLit != null) {
                        final int month = monthLit.intValue();
                        sink.add("month:" + year + "-" + month);
                        final Literal dayLit = model.filter(owltimeDesc, OWLTIME_DAY, null)
                                .objectLiteral();
                        if (dayLit != null) {
                            final int day = dayLit.intValue();
                            sink.add("day:" + year + "-" + month + "-" + day);
                        }
                    }
                }

            } catch (final Throwable ex) {
                // Log and ignore
                LOGGER.warn("Could not extract date components from OWL Time description "
                        + owltimeDesc, ex);
            }
        }

        private static int extractSentenceFromOffset(final KAFDocument document,
                final int sentence) {
            final List<ixa.kaflib.Term> terms = document.getTermsBySent(sentence);
            if (terms == null || terms.isEmpty()) {
                return extractSentenceToOffset(document, document.getNumSentences());
            }
            int offset = Integer.MAX_VALUE;
            for (final ixa.kaflib.Term term : terms) {
                offset = Math.min(offset, term.getOffset());
            }
            return offset;
        }

        private static int extractSentenceToOffset(final KAFDocument document,
                final int sentence) {
            for (int s = sentence; s >= 0; --s) {
                final List<ixa.kaflib.Term> terms = document.getTermsBySent(s);
                if (terms != null && !terms.isEmpty()) {
                    int offset = Integer.MIN_VALUE;
                    for (final ixa.kaflib.Term term : terms) {
                        offset = Math.max(offset, term.getOffset() + term.getLength());
                    }
                    return offset;
                }
            }
            return 0;
        }

        private static boolean containsMention(final QuadModel model, final int fromOffset,
                final int toOffset, final URI mention) {

            int mentionFromOffset, mentionToOffset;

            try {
                final String mentionStr = mention.stringValue();
                final int index3 = mentionStr.lastIndexOf(',');
                final int index1 = mentionStr.lastIndexOf('=', index3);
                final int index2 = mentionStr.indexOf(',', index1);
                mentionFromOffset = Integer.parseInt(mentionStr.substring(index1 + 1, index2));
                mentionToOffset = Integer.parseInt(mentionStr.substring(index3 + 1));
            } catch (final Throwable ex) {
                try {
                    mentionFromOffset = model.filter(mention, NIF_BEGIN_INDEX, null)
                            .objectLiteral().intValue();
                    mentionToOffset = model.filter(mention, NIF_END_INDEX, null).objectLiteral()
                            .intValue();
                } catch (final Throwable ex2) {
                    ex.addSuppressed(ex2);
                    LOGGER.warn("Could not identify begin and end index of mention: " + mention
                            + " (ignoring)", ex);
                    return false;
                }
            }

            return mentionFromOffset >= fromOffset && mentionToOffset <= toOffset;
        }

    }

}
