package eu.fbk.pikesir;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SmallFloat;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ixa.kaflib.KAFDocument;

import eu.fbk.pikesir.util.CommandLine;
import eu.fbk.pikesir.util.Util;
import eu.fbk.rdfpro.AbstractRDFHandlerWrapper;
import eu.fbk.rdfpro.RDFHandlers;
import eu.fbk.rdfpro.RDFSources;
import eu.fbk.rdfpro.util.Environment;
import eu.fbk.rdfpro.util.IO;
import eu.fbk.rdfpro.util.QuadModel;
import eu.fbk.rdfpro.util.Statements;

public class PikesIR {

    private static final Logger LOGGER = LoggerFactory.getLogger(PikesIR.class);

    private static final Pattern NAF_PATTERN = Pattern.compile("\\.naf(\\.(gz|bz2|xz|7z))?$");

    private static final Pattern RDF_PATTERN = Pattern
            .compile("\\.(rdf|rj|jsonld|nt|nq|trix|trig|tql|ttl|n3|brf)" + "(\\.(gz|bz2|xz|7z))?$");

    private final Path pathDocsNAF;

    private final Path pathDocsRDF;

    private final Path pathDocsRDFE;

    private final Path pathDocsTerms;

    private final Path pathQueriesNAF;

    private final Path pathQueriesRDF;

    private final Path pathQueriesRDFE;

    private final Path pathQueriesTerms;

    private final Path pathQueriesRelevances;

    private final Path pathIndex;

    private final Path pathResults;

    private final Set<Field> weightedFields;

    private final List<String> layers;

    private final Multimap<String, Field> layerFields;

    private final Map<String, Integer> layerRepetitions;

    private final Map<String, Double> layerBoosts;

    private final Set<Set<String>> layerFocus;

    private final RankingScore.Measure sortMeasure;

    @Nullable
    private final Integer sortMeasureAtNumber;

    private final Enricher enricher;

    private final Analyzer analyzer;

    private final Aggregator aggregator;

    public static void main(final String... args) {

        try {
            // Parse command line
            final CommandLine cmd = CommandLine
                    .parser()
                    .withName("pikesir")
                    .withOption("p", "properties", "specifies the configuration properties file",
                            "PATH", CommandLine.Type.FILE_EXISTING, true, false, false)
                    .withOption("e", "enrich", "enriches the RDF of both documents and queries")
                    .withOption(null, "enrich-docs", "enriches the RDF of documents only")
                    .withOption(null, "enrich-queries", "enriches the RDF of queries only")
                    .withOption("a", "analyze",
                            "analyzes both documents and queries (NAF + RDF enriched)")
                    .withOption(null, "analyze-docs",
                            "analyzes documents only (NAF + RDF enriched)")
                    .withOption(null, "analyze-queries",
                            "analyzes queries only (NAF + RDF enriched)")
                    .withOption("i", "index", "indexes document terms in Lucene")
                    .withOption("s", "search", "evaluates queries over Lucene index")
                    .withHeader("supports all the operations involved in the evaluation of " //
                            + "semantic information retrieval: enrichment, analysis, " //
                            + "indexing, search").parse(args);

            // Extract options
            final Path propertiesPath = Paths.get(cmd.getOptionValue("p", String.class,
                    System.getProperty("user.dir") + "/pikesir.properties"));
            boolean enrichDocs = cmd.hasOption("enrich-docs") || cmd.hasOption("e");
            boolean enrichQueries = cmd.hasOption("enrich-queries") || cmd.hasOption("e");
            boolean analyzeDocs = cmd.hasOption("analyze-docs") || cmd.hasOption("a");
            boolean analyzeQueries = cmd.hasOption("analyze-queries") || cmd.hasOption("a");
            boolean index = cmd.hasOption("i");
            boolean search = cmd.hasOption("s");

            // Abort if properties file does not exist
            if (!Files.exists(propertiesPath)) {
                throw new CommandLine.Exception("Properties file '" + propertiesPath
                        + "' does not exist");
            }

            // Read properties
            final Properties properties = new Properties();
            try (Reader reader = IO.utf8Reader(IO.buffer(IO.read(propertiesPath.toString())))) {
                properties.load(reader);
            }

            // Force certain actions if specified in properties file
            final String pr = "pikesir.forcecmd.";
            enrichDocs |= Boolean.parseBoolean(properties.getProperty(pr + "enrichdocs", "false"));
            enrichQueries |= Boolean.parseBoolean(properties.getProperty( //
                    pr + "enrichqueries", "false"));
            analyzeDocs |= Boolean.parseBoolean(properties.getProperty( //
                    pr + "analyzedocs", "false"));
            analyzeQueries |= Boolean.parseBoolean(properties.getProperty( //
                    pr + "analyzequeries", "false"));
            index |= Boolean.parseBoolean(properties.getProperty(pr + "index", "false"));
            search |= Boolean.parseBoolean(properties.getProperty(pr + "search", "false"));

            // Initialize the PikesIR main object
            final PikesIR pikesIR = new PikesIR(propertiesPath.getParent(), properties, "pikesir.");

            // Perform the requested operations
            if (enrichQueries) {
                pikesIR.enrichQueries();
            }
            if (enrichDocs) {
                pikesIR.enrichDocs();
            }
            if (analyzeQueries) {
                pikesIR.analyzeQueries();
            }
            if (analyzeDocs) {
                pikesIR.analyzeDocs();
            }
            if (index) {
                pikesIR.index();
            }
            if (search) {
                pikesIR.search();
            }

        } catch (final Throwable ex) {
            // Display error information and terminate
            CommandLine.fail(ex);
        }
    }

    public PikesIR(final Path root, final Properties properties, final String prefix) {

        // Normalize prefix, ensuring it ends with '.'
        final String pr = prefix.endsWith(".") ? prefix : prefix + ".";

        // Retrieve document paths
        this.pathDocsNAF = root.resolve(properties.getProperty(pr + "docs.naf", "docs/naf"));
        this.pathDocsRDF = root.resolve(properties.getProperty(pr + "docs.rdf", "docs/rdf"));
        this.pathDocsRDFE = root.resolve(properties.getProperty(pr + "docs.rdfe", "docs/rdfe"));
        this.pathDocsTerms = root.resolve(properties.getProperty(pr + "docs.terms",
                "docs/terms.tsv.gz"));

        // Retrieve queries paths
        this.pathQueriesNAF = root.resolve(properties.getProperty( //
                pr + "queries.naf", "queries/naf"));
        this.pathQueriesRDF = root.resolve(properties.getProperty( //
                pr + "queries.rdf", "queries/rdf"));
        this.pathQueriesRDFE = root.resolve(properties.getProperty( //
                pr + "queries.rdfe", "queries/rdfe"));
        this.pathQueriesTerms = root.resolve(properties.getProperty( //
                pr + "queries.terms", "queries/terms.tsv.gz"));
        this.pathQueriesRelevances = root.resolve(properties.getProperty( //
                pr + "queries.relevances", "queries/relevances.tsv.gz"));

        // Retrieve index path
        this.pathIndex = root.resolve(properties.getProperty(pr + "index", "index"));

        // Retrieve results path
        this.pathResults = root.resolve(properties.getProperty(pr + "results", "results"));

        // Retrieve weighted fields
        this.weightedFields = Sets.newEnumSet(ImmutableSet.of(), Field.class);
        for (final String fieldSpec : properties.getProperty(pr + "index.weightedfields", "")
                .split("[\\s,;]+")) {
            this.weightedFields.add(Field.forID(fieldSpec.trim()));
        }

        // Retrieve layers and associated fields
        this.layers = Lists.newArrayList();
        this.layerFields = HashMultimap.create();
        for (final String layerSpec : Splitter.on(';').trimResults().omitEmptyStrings()
                .split(properties.getProperty(pr + "layers"))) {
            final int index = layerSpec.indexOf(':');
            final String layer = layerSpec.substring(0, index).trim();
            this.layers.add(layer);
            for (final String fieldSpec : Splitter.on(Pattern.compile("[\\s,]+"))
                    .omitEmptyStrings().trimResults().split(layerSpec.substring(index + 1))) {
                final Field field = Field.forID(fieldSpec.trim());
                this.layerFields.put(layer, field);
            }
        }

        // Retrieve layer boosts and repetitions settings
        this.layerBoosts = Util.parseMap(properties.getProperty(pr + "layers.boosts"),
                Double.class);
        this.layerRepetitions = Util.parseMap(properties.getProperty(pr + "layers.repetitions"),
                Integer.class);

        // Retrieve the layer combinations we focus on
        this.layerFocus = Sets.newHashSet();
        for (final String spec : Splitter.on(';').trimResults().omitEmptyStrings()
                .split(properties.getProperty(pr + "results.focus", ""))) {
            this.layerFocus.add(ImmutableSet.copyOf(Splitter.on(',').trimResults()
                    .omitEmptyStrings().split(spec)));
        }

        // Retrieve sort preferences
        final String sortSpec = properties.getProperty(pr + "results.sort", "map").trim()
                .toUpperCase();
        final int index = sortSpec.indexOf('@');
        this.sortMeasure = RankingScore.Measure.valueOf(index < 0 ? sortSpec : sortSpec.substring(
                0, index));
        this.sortMeasureAtNumber = index < 0 ? null : Integer.parseInt(sortSpec
                .substring(index + 1));

        // Build the enricher
        this.enricher = Enricher.create(root, properties, "pikesir.enricher.");

        // Build the analyzer
        this.analyzer = Analyzer.create(root, properties, "pikesir.analyzer.");

        // Build the aggregator
        this.aggregator = Aggregator.create(root, properties, "pikesir.aggregator.");

        // Report configuration
        LOGGER.info("Focus on layer combinations: " + Joiner.on(' ').join(this.layerFocus));
        LOGGER.info("Using enricher: {}", this.enricher);
        LOGGER.info("Using analyzer: {}", this.analyzer);
        LOGGER.info("Using aggregator: {}", this.aggregator);
    }

    public void enrichDocs() throws IOException {
        enrichHelper(this.pathDocsRDF, this.pathDocsRDFE, "=== Enriching documents ===");
    }

    public void enrichQueries() throws IOException {
        enrichHelper(this.pathQueriesRDF, this.pathQueriesRDFE, "=== Enriching queries ===");
    }

    private void enrichHelper(final Path pathSource, final Path pathDest, final String message)
            throws IOException {

        final long ts = System.currentTimeMillis();
        final AtomicLong inTriples = new AtomicLong(0L);
        final AtomicLong outTriples = new AtomicLong(0L);

        LOGGER.info(message);

        final int prefixLength = pathSource.toAbsolutePath().toString().length() + 1;
        Files.createDirectories(pathDest);
        forEachFile(pathSource, RDF_PATTERN, (final Path path) -> {
            final String relativePath = path.toAbsolutePath().toString().substring(prefixLength);
            final int nameEnd = indexOf(relativePath, RDF_PATTERN);
            final Path outputPath = pathDest.resolve( //
                    relativePath.substring(0, nameEnd) + ".tql.gz");
            try {
                final QuadModel model = readTriples(path);
                final int sizeBefore = model.size();
                this.enricher.enrich(model);
                writeTriples(outputPath, model);
                inTriples.addAndGet(sizeBefore);
                outTriples.addAndGet(model.size());
                LOGGER.info("Enriched {} - {} triples obtained from {} triples", path, sizeBefore,
                        model.size());
            } catch (final Throwable ex) {
                Throwables.propagate(ex);
            }
        });

        LOGGER.info("Done in {} ms ({} triples in, {} triples out)", System.currentTimeMillis()
                - ts, inTriples, outTriples);
    }

    public void analyzeDocs() throws IOException {
        analyzeHelper(this.pathDocsNAF, this.pathDocsRDFE, this.pathDocsTerms,
                "=== Analyzing documents ===", false);
    }

    public void analyzeQueries() throws IOException {
        analyzeHelper(this.pathQueriesNAF, this.pathQueriesRDFE, this.pathQueriesTerms,
                "=== Analyzing queries ===", true);
    }

    private void analyzeHelper(final Path pathNAF, final Path pathRDFE, final Path pathTerms,
            final String message, final boolean isQuery) throws IOException {

        final long ts = System.currentTimeMillis();
        final AtomicLong outTerms = new AtomicLong(0L);

        LOGGER.info(message);

        final int nafPrefixLength = pathNAF.toAbsolutePath().toString().length() + 1;
        try (Writer writer = IO.utf8Writer(IO.buffer(IO.write(pathTerms.toAbsolutePath()
                .toString())))) {
            forEachFile(pathNAF, NAF_PATTERN, (final Path path) -> {
                final String relativePath = path.toAbsolutePath().toString() //
                        .substring(nafPrefixLength);
                final int nameEnd = indexOf(relativePath, NAF_PATTERN);
                final Path rdfePath = pathRDFE.resolve( //
                        relativePath.substring(0, nameEnd) + ".tql.gz");
                try {
                    final QuadModel model = readTriples(rdfePath);
                    byte[] bytes;
                    try (InputStream stream = IO.read( //
                            path.toAbsolutePath().toString())) {
                        bytes = ByteStreams.toByteArray(stream);
                    }
                    final KAFDocument document;
                    document = KAFDocument.createFromStream( //
                            IO.utf8Reader(new ByteArrayInputStream(bytes)));
                    final String id = document.getPublic().publicId;
                    final TermVector.Builder builder = TermVector.builder();
                    this.analyzer.analyze(document, model, builder, isQuery);
                    final TermVector vector = builder.build();
                    outTerms.addAndGet(vector.size());
                    synchronized (writer) {
                        TermVector.write(writer, ImmutableMap.of(id, vector));
                    }
                    LOGGER.info("Analyzed {} - {} terms from {} tokens, {} triples", path,
                            vector.size(), document.getTerms().size(), model.size());
                } catch (final Throwable ex) {
                    Throwables.propagate(ex);
                }
            });
        }

        LOGGER.info("Done in {} ms ({} terms out)", System.currentTimeMillis() - ts, outTerms);
    }

    public void index() throws IOException {

        final long ts = System.currentTimeMillis();
        LOGGER.info("=== Building Lucene index ===");

        // Create index directory if necessary and wipe out existing directory contents
        initDir(this.pathIndex);

        // TODO: avoid loading everything into RAM; parallelize
        Map<String, TermVector> vectors = Maps.newHashMap();
        try (Reader reader = IO.utf8Reader(IO.buffer(IO.read(this.pathDocsTerms.toAbsolutePath()
                .toString())))) {
            vectors = TermVector.read(reader);
        }

        // Lucene 4.10.3
        //final FSDirectory indexDir = FSDirectory.open(this.pathIndex.toFile());
        //final IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_10_3,
        //        new KeywordAnalyzer());
        final FSDirectory indexDir = FSDirectory.open(this.pathIndex);
        final IndexWriterConfig config = new IndexWriterConfig(new CustomAnalyzer());
        config.setSimilarity(CustomSimilarity.INSTANCE);

        int numTerms = 0;
        try (IndexWriter writer = new IndexWriter(indexDir, config)) {
            for (final Map.Entry<String, TermVector> entry : vectors.entrySet()) {
                LOGGER.info("Indexing {} - {} terms", entry.getKey(), entry.getValue().size());
                final Document doc = new Document();
                doc.add(new TextField("id", entry.getKey(), Store.YES));
                for (final Term term : entry.getValue().getTerms()) {
                    final String fieldID = term.getField().getID();
                    final String fieldValue = "|" + term.getWeight() + "| " + term.getValue();
                    // final String fieldValue = term.getValue();
                    if (this.weightedFields.contains(term.getField())) {
                        for (int i = 0; i < (int) Math.ceil(term.getWeight()); ++i) {
                            doc.add(new TextField(fieldID, fieldValue, Store.YES));
                        }
                    } else {
                        doc.add(new TextField(fieldID, fieldValue, Store.YES));
                    }
                    ++numTerms;
                }
                writer.addDocument(doc);
            }
        }

        LOGGER.info("Done in {} ms ({} documents, {} terms added)", System.currentTimeMillis()
                - ts, vectors.size(), numTerms);
    }

    public void search() throws IOException {

        final long ts = System.currentTimeMillis();
        LOGGER.info("=== Searching Lucene index ===");

        // Read relevances
        final Map<String, Map<String, Double>> rels = readRelevances(this.pathQueriesRelevances);

        // Read queries
        Map<String, TermVector> queries = Maps.newHashMap();
        try (Reader reader = IO.utf8Reader(IO.buffer(IO.read(this.pathQueriesTerms
                .toAbsolutePath().toString())))) {
            queries = TermVector.read(reader);
        }

        // Materialize all possible layer combinations and associated evaluators
        final Map<List<String>, Map<String, RankingScore>> scores = Maps.newHashMap();
        for (final Set<String> combination : Sets.powerSet(ImmutableSet.copyOf(this.layers))) {
            if (!combination.isEmpty()) {
                final List<String> sortedLayers = Lists.newArrayList();
                for (final String layer : this.layers) {
                    if (combination.contains(layer)) {
                        sortedLayers.add(layer);
                    }
                }
                scores.put(sortedLayers, Maps.newHashMap());
            }
        }

        // Create results directory if necessary and wipe out existing content
        initDir(this.pathResults);

        // Open the index for read and evaluate all the queries in parallel
        try (IndexReader reader = DirectoryReader.open(FSDirectory.open(this.pathIndex))) {
            final IndexSearcher searcher = new IndexSearcher(reader);
            searcher.setSimilarity(CustomSimilarity.INSTANCE);
            final List<Runnable> queryJobs = Lists.newArrayList();
            final Map<Integer, String> docIDs = Maps.newConcurrentMap();
            final Map<String, TermVector> docVectors = Maps.newConcurrentMap();
            for (final String queryID : Ordering.natural().sortedCopy(queries.keySet())) {
                final TermVector queryVector = queries.get(queryID);
                final Map<String, Double> queryRels = rels.get(queryID);
                queryJobs.add(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            searchHelper(searcher, queryID, queryVector, queryRels, scores,
                                    docIDs, docVectors);
                        } catch (final Throwable ex) {
                            throw Throwables.propagate(ex);
                        }
                    }

                });
            }
            Environment.run(queryJobs);
        }

        // Compute aggregated scores and sort them
        final Map<RankingScore, List<String>> aggregateScores = Maps.newIdentityHashMap();
        for (final Map.Entry<List<String>, Map<String, RankingScore>> entry : scores.entrySet()) {
            aggregateScores.put(RankingScore.average(entry.getValue().values()), entry.getKey());
        }
        final List<RankingScore> sortedScores = RankingScore.comparator(this.sortMeasure,
                this.sortMeasureAtNumber, true).sortedCopy(aggregateScores.keySet());

        // Write aggregate scores
        try (Writer writer = IO.utf8Writer(IO.buffer(IO.write(this.pathResults
                .resolve("aggregates.csv").toAbsolutePath().toString())))) {
            writer.append("layers;p@1;p@3;p@5;p@10;mrr;ndcg;ndcg@10;map;map@10\n");
            for (final RankingScore score : sortedScores) {
                writer.append(Joiner.on(",").join(aggregateScores.get(score))).append(";")
                        .append(formatRankingScore(score, ";")).append("\n");
            }
        }

        // Write reports for the layer combinations we focus on
        for (final Set<String> layers : this.layerFocus) {
            try (Writer writer = IO.utf8Writer(IO.buffer(IO.write(this.pathResults
                    .resolve("queries-" + Joiner.on('-').join(layers) + ".csv").toAbsolutePath()
                    .toString())))) {
                writer.append("query;p@1;p@3;p@5;p@10;mrr;ndcg;ndcg@10;map;map@10\n");
                for (final Map.Entry<List<String>, Map<String, RankingScore>> entry : scores
                        .entrySet()) {
                    if (ImmutableSet.copyOf(entry.getKey()).equals(layers)) {
                        for (final String queryID : Ordering.natural().sortedCopy(
                                entry.getValue().keySet())) {
                            writer.append(queryID)
                                    .append(';')
                                    .append(formatRankingScore(entry.getValue().get(queryID), ";"))
                                    .append('\n');
                        }
                    }
                }
            }
        }

        // Report top aggregate scores
        if (LOGGER.isInfoEnabled()) {
            final StringBuilder builder = new StringBuilder("Top scores:");
            for (int i = 0; i < 10; ++i) {
                final RankingScore score = sortedScores.get(i);
                builder.append(String.format("\n  %-40s - p@1=%.3f p@3=%.3f p@5=%.3f p@10=%.3f "
                        + "mrr=%.3f ndcg=%.3f ndcg@10=%.3f map=%.3f map@10=%.3f", Joiner.on(',')
                        .join(aggregateScores.get(score)), score.getPrecision(1), score
                        .getPrecision(3), score.getPrecision(5), score.getPrecision(10), score
                        .getMRR(), score.getNDCG(), score.getNDCG(10), score.getMAP(), score
                        .getMAP(10)));
            }
            LOGGER.info(builder.toString());
        }

        LOGGER.info("Done in {} ms", System.currentTimeMillis() - ts);
    }

    private void searchHelper(final IndexSearcher searcher, final String queryID,
            final TermVector queryVector, final Map<String, Double> rels,
            final Map<List<String>, Map<String, RankingScore>> scores,
            final Map<Integer, String> docIDs, final Map<String, TermVector> docVectors)
            throws IOException, ParseException {

        // Perform the query on each layer, storing all the hits obtained
        final Map<String, Hit> hits = Maps.newHashMap();
        final Set<String> availableLayers = Sets.newHashSet();
        for (final String layer : this.layers) {

            // Retrieve boost and repetitions for current layer
            final double boost = this.layerBoosts.getOrDefault(layer, 1.0);
            final int repetitions = this.layerRepetitions.getOrDefault(layer, 1);

            // Compose query
            final StringBuilder builder = new StringBuilder();
            String separator = "";
            for (final Field field : this.layerFields.get(layer)) {
                for (final Term term : queryVector.getTerms(field)) {
                    for (int i = 0; i < repetitions; ++i) {
                        builder.append(separator);
                        builder.append(field.getID()).append(":\"").append(term.getValue())
                                .append("\"");
                        if (boost != 1.0) {
                            builder.append("^").append(boost);
                        }
                        separator = " OR ";
                    }
                }
            }
            final String queryString = builder.toString();

            // If query is non-empty, mark the layer as available and perform the query
            if (!queryString.isEmpty()) {
                availableLayers.add(layer);
                final QueryParser parser = new QueryParser("default-field", new CustomAnalyzer());
                final Query query = parser.parse(queryString);
                final TopDocs results = searcher.search(query, 1000);
                LOGGER.debug("{} results obtained from query {}", results.scoreDocs.length,
                        queryString);
                for (final ScoreDoc scoreDoc : results.scoreDocs) {
                    String docID = docIDs.get(scoreDoc.doc);
                    if (docID == null) {
                        final Document doc = searcher.doc(scoreDoc.doc);
                        docID = searcher.doc(scoreDoc.doc, ImmutableSet.of("id")).get("id");
                        docIDs.put(scoreDoc.doc, docID);
                        final TermVector docVector = toTermVector(doc);
                        docVectors.put(docID, docVector);
                    }
                    Hit hit = hits.get(docID);
                    if (hit == null) {
                        hit = new Hit(docID);
                        hits.put(docID, hit);
                    }
                    hit.setLayerScore(layer, scoreDoc.score);
                }
            }
        }

        final Map<RankingScore, String> queryLines = Maps.newIdentityHashMap();
        RankingScore bestScore = null;
        List<String> bestLayers = null;

        for (final Map.Entry<List<String>, Map<String, RankingScore>> entry : scores.entrySet()) {

            final List<String> allLayers = entry.getKey();
            final List<String> queryLayers = Lists.newArrayList();
            for (final String layer : allLayers) {
                if (availableLayers.contains(layer)) {
                    queryLayers.add(layer);
                }
            }

            final Map<String, RankingScore> queryScores = entry.getValue();
            if (!queryLayers.isEmpty()) {
                final List<Hit> sortedHits = Lists.newArrayListWithCapacity(hits.size());
                final List<TermVector> sortedDocVectors = Lists.newArrayListWithCapacity(hits
                        .size());
                for (final Hit hit : hits.values()) {
                    for (final String layer : queryLayers) {
                        if (hit.getLayerScore(layer) != 0.0) {
                            sortedHits.add(hit);
                            sortedDocVectors.add(docVectors.get(hit.getDocumentID()));
                            break;
                        }
                    }
                }
                this.aggregator.aggregate(allLayers, queryLayers, queryVector, sortedHits,
                        sortedDocVectors);
                Collections.sort(sortedHits, Hit.comparator(null, true));
                final List<String> ids = Lists.newArrayListWithCapacity(sortedHits.size());
                for (final Hit hit : sortedHits) {
                    ids.add(hit.getDocumentID());
                }

                final RankingScore score = RankingScore.evaluator(10).add(ids, rels).get();
                queryScores.put(queryID, score);

                queryLines.put(score,
                        Joiner.on(',').join(queryLayers) + ';' + formatRankingScore(score, ";")
                                + ';' + Joiner.on(",").join(Iterables.limit(sortedHits, 10)));

                if (bestScore == null
                        || RankingScore.comparator(this.sortMeasure, this.sortMeasureAtNumber,
                                true).compare(score, bestScore) < 0) {
                    bestScore = score;
                    bestLayers = allLayers;
                }

            } else {
                // A rescale factor was previously used here
                queryScores.put(queryID, RankingScore.evaluator(10).add(ImmutableList.of(), rels)
                        .get());
            }
        }

        try (Writer writer = IO.utf8Writer(IO.buffer(IO.write(this.pathResults
                .resolve(queryID + ".csv").toAbsolutePath().toString())))) {
            writer.append("layers;p@1;p@3;p@5;p@10;mrr;ndcg;ndcg@10;map;map@10;ranking (top 10)\n");
            for (final RankingScore score : RankingScore.comparator(this.sortMeasure,
                    this.sortMeasureAtNumber, true).sortedCopy(queryLines.keySet())) {
                writer.append(queryLines.get(score)).append('\n');
            }
        }

        try (Writer writer = IO.utf8Writer(IO.buffer(IO.write(this.pathResults
                .resolve(queryID + "-rank.csv").toAbsolutePath().toString())))) {
            final List<List<Hit>> sortedHits = Lists.newArrayList();
            for (final String layer : this.layers) {
                sortedHits.add(Hit.comparator(layer, true).sortedCopy(hits.values()));
                writer.append(layer).append(";;;");
            }
            writer.append(";;\n");
            final int numLayers = this.layers.size();
            final int numRows = Math.min(50, hits.size());
            final String[] rowIDs = new String[numLayers];
            final double[] rowScores = new double[numLayers];
            for (int i = 0; i < numRows; ++i) {
                for (int j = 0; j < numLayers; ++j) {
                    final String layer = this.layers.get(j);
                    final Hit hit = sortedHits.get(j).get(i);
                    rowIDs[j] = hit.getDocumentID();
                    rowScores[j] = hit.getLayerScore(layer);
                }
                if (Doubles.max(rowScores) == 0.0) {
                    break;
                }
                for (int j = 0; j < numLayers; ++j) {
                    final double score = rowScores[j];
                    if (score == 0.0) {
                        writer.append(";;;");
                    } else {
                        final String id = rowIDs[j];
                        final Double rel = rels.get(id);
                        writer.append(id).append(rel == null ? "" : " (" + rel + ")").append(';')
                                .append(Double.toString(score)).append(";;");
                    }
                }
                writer.append("\n");
            }
        }

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Evaluated {} - {} hits, best: {} {}", queryID,
                    String.format("%4d", hits.size()), bestScore, Joiner.on(',').join(bestLayers));
        }
    }

    private static TermVector toTermVector(final Document document) {
        final TermVector.Builder builder = TermVector.builder();
        for (final IndexableField field : document.getFields()) {
            final String name = field.name();
            if (!"id".equals(name)) {
                final String value = field.stringValue();
                try {
                    final Field f = Field.forID(name);
                    builder.addTerm(f, value);
                } catch (final Throwable ex) {
                    // Ignore
                }
            }
        }
        final TermVector vector = builder.build();
        return vector;
    }

    private static String formatRankingScore(final RankingScore score, final String separator) {
        final StringBuilder builder = new StringBuilder();
        builder.append(score.getPrecision(1)).append(separator);
        builder.append(score.getPrecision(3)).append(separator);
        builder.append(score.getPrecision(5)).append(separator);
        builder.append(score.getPrecision(10)).append(separator);
        builder.append(score.getMRR()).append(separator);
        builder.append(score.getNDCG()).append(separator);
        builder.append(score.getNDCG(10)).append(separator);
        builder.append(score.getMAP()).append(separator);
        builder.append(score.getMAP(10));
        return builder.toString();
    }

    private static Map<String, Map<String, Double>> readRelevances(final Path path)
            throws IOException {

        final Map<String, Map<String, Double>> rels = Maps.newHashMap();
        for (final String line : Files.readAllLines(path)) {
            final String[] tokens = line.split("[\\s+,;]+");
            final Map<String, Double> map = Maps.newHashMap();
            rels.put(tokens[0], map);
            for (int i = 1; i < tokens.length; ++i) {
                final int j = tokens[i].lastIndexOf(':');
                if (j < 0) {
                    map.put(tokens[i], 1.0);
                } else {
                    map.put(tokens[i].substring(0, j),
                            Double.parseDouble(tokens[i].substring(j + 1)));
                }
            }
        }
        return rels;
    }

    private static QuadModel readTriples(final Path path) throws IOException {
        try {
            final QuadModel model = QuadModel.create();
            RDFSources.read(false, true, null, null, path.toAbsolutePath().toString()).emit(
                    new AbstractRDFHandlerWrapper(RDFHandlers.wrap(model)) {

                        @Override
                        public void handleStatement(final Statement stmt)
                                throws RDFHandlerException {
                            super.handleStatement(Statements.VALUE_FACTORY.createStatement(
                                    stmt.getSubject(), stmt.getPredicate(), stmt.getObject()));
                        }

                    }, 1);
            return model;
        } catch (final RDFHandlerException ex) {
            throw new IOException(ex);
        }
    }

    private static void writeTriples(final Path path, final Iterable<Statement> model)
            throws IOException {
        try {
            Files.createDirectories(path.getParent());
            RDFSources.wrap(model).emit(
                    RDFHandlers.write(null, 1000, path.toAbsolutePath().toString()), 1);
        } catch (final RDFHandlerException ex) {
            throw new IOException(ex);
        }
    }

    private static void forEachFile(final Path path, final Pattern pattern,
            final Consumer<Path> consumer) {

        final FluentIterable<File> files = com.google.common.io.Files.fileTreeTraverser()
                .preOrderTraversal(path.toFile()).filter((final File file) -> {
                    return file.isFile() && pattern.matcher(file.getName()).find();
                });

        final int count = files.size();
        LOGGER.info("Processing {} files", count);

        Environment.run(files.<Runnable>transform((final File file) -> new Runnable() {

            @Override
            public void run() {
                try {
                    consumer.accept(file.toPath());
                } catch (final Throwable ex) {
                    throw new RuntimeException("Could not process " + file, ex);
                }
            }

        }));
    }

    private static int indexOf(final String string, final Pattern pattern) {
        final Matcher matcher = pattern.matcher(string);
        if (!matcher.find()) {
            return -1;
        }
        return matcher.start();
    }

    private static void initDir(final Path path) throws IOException {
        Files.createDirectories(path);
        Files.walkFileTree(path, new SimpleFileVisitor<Path>() {

            @Override
            public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs)
                    throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(final Path dir, final IOException exc)
                    throws IOException {
                if (!path.equals(dir)) {
                    Files.delete(dir);
                }
                return FileVisitResult.CONTINUE;
            }

        });
    }

    private static final class CustomSimilarity extends Similarity {

        static final CustomSimilarity INSTANCE = new CustomSimilarity();

        private static final float[] NORM_TABLE = new float[256];

        static {
            for (int i = 0; i < 256; i++) {
                NORM_TABLE[i] = SmallFloat.byte315ToFloat((byte) i);
            }
        }

        @Override
        public float coord(final int overlap, final int maxOverlap) {
            return 1.0f; // modified
        }

        @Override
        public final long computeNorm(final FieldInvertState state) {
            return encodeNormValue(1.0f); // modified
        }

        // default: 1.0 / Math.sqrt(sumOfSquaredWeights)
        @Override
        public float queryNorm(final float sumOfSquaredWeights) {
            return (float) (1.0 / Math.sqrt(sumOfSquaredWeights));
        }

        @Override
        public final SimWeight computeWeight(final CollectionStatistics collectionStats,
                final TermStatistics... termStats) {

            final Explanation idf = termStats.length == 1 ? idfExplain(collectionStats,
                    termStats[0]) : idfExplain(collectionStats, termStats);

            final List<String> tokens = Lists.newArrayListWithCapacity(termStats.length);
            for (int i = 0; i < termStats.length; ++i) {
                tokens.add(termStats[0].term().utf8ToString());
            }

            return new IDFStats(collectionStats.field(), tokens, idf);
        }

        @Override
        public final SimScorer simScorer(final SimWeight stats, final LeafReaderContext context)
                throws IOException {
            final IDFStats idfstats = (IDFStats) stats;
            return new TFIDFSimScorer(idfstats, context);
        }

        private float tf(final float freq) {
            return (float) Math.sqrt(freq);
        }

        private Explanation idfExplain(final CollectionStatistics collectionStats,
                final TermStatistics termStats) {
            final long df = termStats.docFreq();
            final long max = collectionStats.maxDoc();
            final float idf = idf(df, max);
            return Explanation.match(idf, "idf(docFreq=" + df + ", maxDocs=" + max + ")");
        }

        private Explanation idfExplain(final CollectionStatistics collectionStats,
                final TermStatistics termStats[]) {
            final long max = collectionStats.maxDoc();
            float idf = 0.0f;
            final List<Explanation> subs = new ArrayList<>();
            for (final TermStatistics stat : termStats) {
                final long df = stat.docFreq();
                final float termIdf = idf(df, max);
                subs.add(Explanation
                        .match(termIdf, "idf(docFreq=" + df + ", maxDocs=" + max + ")"));
                idf += termIdf;
            }
            return Explanation.match(idf, "idf(), sum of:", subs);
        }

        private float idf(final long docFreq, final long numDocs) {
            return (float) (Math.log(numDocs / (double) (docFreq + 1)) + 1.0);
        }

        private final float decodeNormValue(final long norm) {
            return NORM_TABLE[(int) (norm & 0xFF)]; // & 0xFF maps negative bytes to positive above 127
        }

        private final long encodeNormValue(final float f) {
            return SmallFloat.floatToByte315(f);
        }

        private float sloppyFreq(final int distance) {
            return 1.0f / (distance + 1);
        }

        private final class TFIDFSimScorer extends SimScorer {

            private final LeafReaderContext context;

            private final IDFStats stats;

            private final float weightValue;

            private final NumericDocValues norms;

            TFIDFSimScorer(final IDFStats stats, final LeafReaderContext context)
                    throws IOException {
                this.context = context;
                this.stats = stats;
                this.weightValue = stats.value;
                this.norms = this.context.reader().getNormValues(stats.field);
            }

            @Override
            public float score(final int doc, final float freq) {

                // UNCOMMENT HERE TO OBTAIN WEIGHTS FROM INDEXED PAYLOADS
                //    float weight = 1.0f;
                //    try {
                //        final PostingsEnum pe = this.context.reader().postings(
                //                new org.apache.lucene.index.Term(this.stats.field,
                //                        this.stats.tokens.get(0)), PostingsEnum.PAYLOADS);
                //        pe.advance(doc);
                //        pe.nextPosition();
                //        final BytesRef payload = pe.getPayload();
                //        if (payload != null) {
                //            weight = Float.intBitsToFloat(Ints.fromByteArray(payload.bytes));
                //        }
                //    } catch (final IOException ex) {
                //        Throwables.propagate(ex);
                //    }

                final float raw = tf(freq) * this.weightValue; // compute tf(f)*weight
                return this.norms == null ? raw : raw * decodeNormValue(this.norms.get(doc)); // normalize for field
            }

            @Override
            public float computeSlopFactor(final int distance) {
                return sloppyFreq(distance);
            }

            @Override
            public float computePayloadFactor(final int doc, final int start, final int end,
                    final BytesRef payload) {
                return 1.0f; // never called (!)
            }

            @Override
            public Explanation explain(final int doc, final Explanation freq) {
                return explainScore(doc, freq, this.stats, this.norms);
            }

        }

        private static class IDFStats extends SimWeight {

            final String field;

            final List<String> tokens;

            final Explanation idf;

            float queryNorm;

            float boost;

            float queryWeight;

            float value;

            public IDFStats(final String field, final List<String> tokens, final Explanation idf) {
                this.field = field;
                this.tokens = tokens;
                this.idf = idf;
                normalize(1f, 1f);
            }

            @Override
            public float getValueForNormalization() {
                return this.queryWeight * this.queryWeight; // sum of squared weights
            }

            @Override
            public void normalize(final float queryNorm, final float boost) {
                this.boost = boost;
                this.queryNorm = queryNorm;
                this.queryWeight = queryNorm * boost * this.idf.getValue();
                this.value = this.queryWeight * this.idf.getValue();
            }

        }

        private Explanation explainQuery(final IDFStats stats) {
            final List<Explanation> subs = new ArrayList<>();

            final Explanation boostExpl = Explanation.match(stats.boost, "boost");
            if (stats.boost != 1.0f) {
                subs.add(boostExpl);
            }
            subs.add(stats.idf);

            final Explanation queryNormExpl = Explanation.match(stats.queryNorm, "queryNorm");
            subs.add(queryNormExpl);

            return Explanation.match(
                    boostExpl.getValue() * stats.idf.getValue() * queryNormExpl.getValue(),
                    "queryWeight, product of:", subs);
        }

        private Explanation explainField(final int doc, final Explanation freq,
                final IDFStats stats, final NumericDocValues norms) {
            final Explanation tfExplanation = Explanation.match(tf(freq.getValue()), "tf(freq="
                    + freq.getValue() + "), with freq of:", freq);
            final Explanation fieldNormExpl = Explanation.match(
                    norms != null ? decodeNormValue(norms.get(doc)) : 1.0f, "fieldNorm(doc=" + doc
                            + ")");

            return Explanation.match(tfExplanation.getValue() * stats.idf.getValue()
                    * fieldNormExpl.getValue(), "fieldWeight in " + doc + ", product of:",
                    tfExplanation, stats.idf, fieldNormExpl);
        }

        private Explanation explainScore(final int doc, final Explanation freq,
                final IDFStats stats, final NumericDocValues norms) {
            final Explanation queryExpl = explainQuery(stats);
            final Explanation fieldExpl = explainField(doc, freq, stats, norms);
            if (queryExpl.getValue() == 1f) {
                return fieldExpl;
            }
            return Explanation.match(queryExpl.getValue() * fieldExpl.getValue(), "score(doc="
                    + doc + ",freq=" + freq.getValue() + "), product of:", queryExpl, fieldExpl);
        }

    }

    private static final class CustomAnalyzer extends org.apache.lucene.analysis.Analyzer {

        @Override
        protected TokenStreamComponents createComponents(final String fieldName) {
            return new TokenStreamComponents(new CustomTokenizer());
        }

    }

    private static final class CustomTokenizer extends Tokenizer {

        private final CharTermAttribute termAtt;

        private final OffsetAttribute offsetAtt;

        private final PayloadAttribute payloadAtt;

        private final char[] weightBuffer;

        private int finalOffset;

        private boolean done;

        public CustomTokenizer() {
            this.termAtt = addAttribute(CharTermAttribute.class);
            this.offsetAtt = addAttribute(OffsetAttribute.class);
            this.payloadAtt = addAttribute(PayloadAttribute.class);
            this.weightBuffer = new char[32];
            this.finalOffset = 0;
            this.done = false;
            this.termAtt.resizeBuffer(256);
        }

        @Override
        public final boolean incrementToken() throws IOException {
            if (!this.done) {
                clearAttributes();
                this.done = true;
                int upto = 0;
                char[] buffer = this.termAtt.buffer();

                float weight = 1.0f;
                int weightLen = 0;
                while (true) {
                    final int c = this.input.read();
                    if (c >= 0) {
                        this.weightBuffer[weightLen++] = (char) c;
                    }
                    if (c == ' ' && weightLen >= 3 && this.weightBuffer[weightLen - 2] == '|') {
                        weight = Float.parseFloat(new String(this.weightBuffer, 1, weightLen - 3));
                        break;
                    } else if (c < 0 || c != '|' && weightLen == 1 || c != '|' && c != '.'
                            && c != ' ' && !Character.isDigit(c)) {
                        System.arraycopy(this.weightBuffer, 0, buffer, 0, weightLen);
                        upto = weightLen;
                        break;
                    }
                }

                while (true) {
                    final int length = this.input.read(buffer, upto, buffer.length - upto);
                    if (length == -1) {
                        break;
                    }
                    upto += length;
                    if (upto == buffer.length) {
                        buffer = this.termAtt.resizeBuffer(1 + buffer.length);
                    }
                }

                this.termAtt.setLength(upto);
                this.finalOffset = correctOffset(upto);
                this.offsetAtt.setOffset(correctOffset(0), this.finalOffset);

                if (weight != 1.0f) {
                    final BytesRef payload = new BytesRef(Ints.toByteArray(Float
                            .floatToIntBits(weight)), 0, 4);
                    this.payloadAtt.setPayload(payload);
                }

                return true;
            }
            return false;
        }

        @Override
        public final void end() throws IOException {
            super.end();
            this.offsetAtt.setOffset(this.finalOffset, this.finalOffset);
        }

        @Override
        public void reset() throws IOException {
            super.reset();
            this.done = false;
        }

    }

}
