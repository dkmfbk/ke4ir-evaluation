package eu.fbk.pikesir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.annotation.Nullable;
import javax.xml.datatype.XMLGregorianCalendar;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.XMLSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tartarus.snowball.SnowballProgram;

import ixa.kaflib.KAFDocument;

import eu.fbk.pikesir.TermVector.Builder;
import eu.fbk.rdfpro.util.QuadModel;

/*
 * For ESWC 2016 we used only the 'SemanticAnalyzer'. The 'TaxonomicAnalyzer' is an attempt at
 * implementing the taxonomic approach by H. Sack & co.
 */

public abstract class Analyzer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Analyzer.class);

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

    public static Analyzer createTextualAnalyzer(final String stemmerClass,
            @Nullable final Iterable<String> stopWords) {
        return new TextualAnalyzer(stemmerClass, stopWords);
    }

    public static Analyzer createSemanticAnalyzer(final boolean expandQueryTypes) {
        return new SemanticAnalyzer(expandQueryTypes);
    }

    public static Analyzer create(final Path root, final Properties properties, String prefix) {

        // Normalize prefix, ensuring it ends with '.'
        prefix = prefix.endsWith(".") ? prefix : prefix + ".";

        // Build a list of analyzers to be later combined
        final List<Analyzer> analyzers = new ArrayList<>();

        // Add an analyzer extracting stems, if enabled
        if (Boolean.parseBoolean(properties.getProperty(prefix + "textual", "false"))) {
            final String stemmerClass = properties.getProperty(prefix + "textual.stemmer");
            final String stopwordsProp = properties.getProperty(prefix + "textual.stopwords");
            final Set<String> stopwords = stopwordsProp == null ? null : ImmutableSet
                    .copyOf(stopwordsProp.split("\\s+"));
            analyzers.add(createTextualAnalyzer(stemmerClass, stopwords));
        }

        // Add a semantic analyzer, if enabled
        if (Boolean.parseBoolean(properties.getProperty(prefix + "semantic", "false"))) {
            final boolean expandQueryTypes = Boolean.parseBoolean(properties.getProperty(prefix
                    + "semantic.expandQueryTypes", "false"));
            analyzers.add(createSemanticAnalyzer(expandQueryTypes));
        }

        // Combine the enrichers
        return concat(analyzers.toArray(new Analyzer[analyzers.size()]));
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
                            builder.addTerm(Field.TEXTUAL, subWordStem);
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

    private static final class SemanticAnalyzer extends Analyzer {

        private static final URI ENTITY_CLASS = new URIImpl(
                "http://dkm.fbk.eu/ontologies/knowledgestore#Entity");

        private static final URI DENOTED_BY = new URIImpl(
                "http://groundedannotationframework.org/gaf#denotedBy");

        private static final URI HAS_DATE_TIME_DESCRIPTION = new URIImpl(
                "http://www.w3.org/TR/owl-time#hasDateTimeDescription");

        private static final Map<String, Field> TYPE_MAP = ImmutableMap.of( //
                "http://dbpedia.org/class/yago/", Field.TYPE //
                // "http://www.ontologyportal.org/SUMO.owl#", Field.TYPE_SUMO, //
                );

        // TODO: check below inclusion / exclusion of FrameBase / NB / PB

        private static final Set<String> FRAME_PREDICATE_NAMESPACES = ImmutableSet.of( //
                "http://framebase.org/ns/"
                //"http://www.newsreader-project.eu/ontologies/propbank/", //
                //"http://www.newsreader-project.eu/ontologies/nombank/",
                );

        private static final Set<String> FRAME_ROLE_NAMESPACES = ImmutableSet.of( //
                "http://framebase.org/ns/", //
                "http://www.newsreader-project.eu/ontologies/propbank/", //
                "http://www.newsreader-project.eu/ontologies/nombank/");

        private final boolean expandQueryTypes;

        SemanticAnalyzer(final boolean expandQueryTypes) {
            this.expandQueryTypes = expandQueryTypes;
        }

        @Override
        public void analyze(final KAFDocument document, final QuadModel model,
                final Builder builder, final boolean isQuery) {

            final Map<URI, List<URI>> parentMap = Maps.newHashMap();

            for (final Resource entity : model.filter(null, RDF.TYPE, ENTITY_CLASS).subjects()) {

                // Obtain number of mentions for current entity
                final int numMentions = model.filter(entity, DENOTED_BY, null).size();

                // Obtain types and predicates
                final Set<URI> types = Sets.newHashSet();
                final Set<URI> predicates = Sets.newHashSet();
                for (final Value value : model.filter(entity, RDF.TYPE, null).objects()) {
                    if (value instanceof URI) {
                        final URI uri = (URI) value;
                        final String ns = uri.getNamespace();
                        if (TYPE_MAP.containsKey(ns)) {
                            types.add(uri);
                        } else if (FRAME_PREDICATE_NAMESPACES.contains(ns)) {
                            predicates.add(uri);
                        }
                    }
                }

                // Obtain role properties
                final Set<URI> roles = Sets.newHashSet();
                for (final Statement stmt : model.filter(entity, null, null)) {
                    if (FRAME_ROLE_NAMESPACES.contains(stmt.getPredicate().getNamespace())) {
                        roles.add(stmt.getPredicate());
                    }
                }

                // Obtain predicate participants
                Set<URI> participants = ImmutableSet.of();
                if (!roles.isEmpty()) {
                    participants = Sets.newHashSet();
                    for (final URI role : roles) {
                        for (final Value value : model.filter(entity, role, null).objects()) {
                            if (value instanceof URI && ((URI) value).getNamespace().equals( //
                                    "http://dbpedia.org/resource/")) {
                                participants.add((URI) value);
                            }
                        }
                    }
                }

                // Obtain date components
                final List<String> times = Lists.newArrayList();
                for (final Resource uri : Iterables.concat(ImmutableList.of(entity), types)) {
                    for (final Statement stmt : model.filter(uri, null, null)) {
                        if (stmt.getObject() instanceof Literal) {
                            final Literal lit = (Literal) stmt.getObject();
                            final URI dt = lit.getDatatype();
                            if (dt.equals(XMLSchema.DATETIME) || dt.equals(XMLSchema.DATE)
                                    || dt.equals(XMLSchema.GYEAR)
                                    || dt.equals(XMLSchema.GYEARMONTH)) {
                                getDateComponents(lit, times, times, times, times, times);
                            }
                        } else if (stmt.getObject() instanceof URI
                                && stmt.getPredicate().equals(HAS_DATE_TIME_DESCRIPTION)) {
                            String str = ((URI) stmt.getObject()).getLocalName();
                            final int index = str.indexOf("_desc");
                            if (index >= 0) {
                                str = str.substring(0, index);
                                if (str.length() >= 4) {
                                    final int year = Integer.parseInt(str.substring(0, 4));
                                    times.add("century:" + year / 100);
                                    times.add("decade:" + year / 10);
                                    times.add("year:" + year);
                                    if (str.length() >= 6) {
                                        final int month = Integer.parseInt(str.substring(4, 6));
                                        times.add("month:" + year + "-" + month);
                                    }
                                }
                            }
                        }
                    }
                }

                // Emit frame terms
                final double frameWeight = isQuery ? (double) numMentions
                        / (participants.size() * predicates.size()) : numMentions;
                for (final URI participant : participants) {
                    for (final URI predicate : predicates) {
                        final String id = predicate.getLocalName() + "__"
                                + participant.getLocalName();
                        builder.addTerm(Field.FRAME, id, frameWeight);
                    }
                }

                // Remove inherited types and roles, if necessary
                if (!this.expandQueryTypes && isQuery) {
                    for (final URI type : ImmutableList.copyOf(types)) {
                        types.removeAll(getParents(type, parentMap, RDFS.SUBCLASSOF, model));
                    }
                }

                // Emit entity term (if belonging to DBpedia)
                if (entity instanceof URI) {
                    final URI uri = (URI) entity;
                    if (uri.getNamespace().equals("http://dbpedia.org/resource/")) {
                        builder.addTerm(Field.URI, uri.getLocalName(), numMentions);
                    }
                }

                // Emit type terms
                for (final URI type : types) {
                    final Field field = TYPE_MAP.get(type.getNamespace());
                    final double weight = isQuery ? (double) numMentions / types.size()
                            : numMentions;
                    builder.addTerm(field, type.getLocalName(), weight);
                }

                // Emit time components
                if (!times.isEmpty()) {
                    if (isQuery) {
                        final Set<String> set = ImmutableSet.copyOf(times);
                        final double weight = (double) numMentions / set.size();
                        for (final String element : set) {
                            builder.addTerm(Field.TIME, element, weight);
                        }
                    } else {
                        for (final String element : ImmutableSet.copyOf(times)) {
                            builder.addTerm(Field.TIME, element, numMentions);
                        }
                    }
                }
            }
        }

        private static List<URI> getParents(final URI uri, final Map<URI, List<URI>> parentMap,
                final URI parentProperty, final QuadModel model) {
            List<URI> parents = parentMap.get(uri);
            if (parents == null) {
                parents = Lists.newArrayList();
                parentMap.put(uri, parents);
                for (final Value parent : model.filter(uri, parentProperty, null).objects()) {
                    if (parent instanceof URI && !parent.equals(uri)) {
                        parents.add((URI) parent);
                    }
                }
            }
            return parents;
        }

        private static void getDateComponents(final Literal literal, final List<String> centuries,
                final List<String> decades, final List<String> years, final List<String> months,
                final List<String> days) {

            try {
                if (!literal.getDatatype().equals(XMLSchema.DATETIME)) {

                    final XMLGregorianCalendar calendarValue = literal.calendarValue();
                    final Integer day = calendarValue.getDay();
                    final Integer month = calendarValue.getMonth();
                    final Integer year = calendarValue.getYear();
                    final Integer decade = year / 10;
                    final Integer century = year / 100;

                    if (literal.getDatatype().equals(XMLSchema.DATE)) {
                        centuries.add("century:" + century);
                        decades.add("decade:" + decade);
                        years.add("year:" + year);
                        months.add("month:" + year + "-" + month);
                        days.add("day:" + year + "-" + month + "-" + day);
                    }

                    if (literal.getDatatype().equals(XMLSchema.GYEARMONTH)) {
                        centuries.add("century:" + century);
                        decades.add("decade:" + decade);
                        years.add("year:" + year);
                        months.add("month:" + year + "-" + month);
                    }

                    if (literal.getDatatype().equals(XMLSchema.GYEAR)) {
                        centuries.add("century:" + century);
                        decades.add("decade:" + decade);
                        years.add("year:" + year);
                    }

                } else {
                    final Integer year = Integer.parseInt(literal.stringValue().substring(0, 4));
                    final Integer month = Integer.parseInt(literal.stringValue().substring(5, 7));
                    final Integer day = Integer.parseInt(literal.stringValue().substring(8, 10));
                    final Integer decade = year / 10;
                    final Integer century = year / 100;

                    centuries.add("century:" + century);
                    decades.add("decade:" + decade);
                    years.add("year:" + year);
                    months.add("month:" + year + "-" + month);
                    days.add("day:" + year + "-" + month + "-" + day);
                }
            } catch (final Throwable ex) {
                // Ignore
                LOGGER.warn("Could not extract date components from " + literal, ex);
            }
        }

    }

}
