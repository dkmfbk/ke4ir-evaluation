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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ixa.kaflib.KAFDocument;

import eu.fbk.pikesir.util.CommandLine;
import eu.fbk.rdfpro.AbstractRDFHandlerWrapper;
import eu.fbk.rdfpro.RDFHandlers;
import eu.fbk.rdfpro.RDFSources;
import eu.fbk.rdfpro.util.Environment;
import eu.fbk.rdfpro.util.IO;
import eu.fbk.rdfpro.util.QuadModel;
import eu.fbk.rdfpro.util.Statements;
import eu.fbk.rdfpro.util.Tracker;

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

    private final List<String> layers;

    private final Multimap<String, String> layerFields;

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
            final boolean enrichDocs = cmd.hasOption("enrich-docs") || cmd.hasOption("e");
            final boolean enrichQueries = cmd.hasOption("enrich-queries") || cmd.hasOption("e");
            final boolean analyzeDocs = cmd.hasOption("analyze-docs") || cmd.hasOption("a");
            final boolean analyzeQueries = cmd.hasOption("analyze-queries") || cmd.hasOption("a");
            final boolean index = cmd.hasOption("i");
            final boolean search = cmd.hasOption("s");

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

        // Retrieve layers and associated fields
        this.layers = Lists.newArrayList();
        this.layerFields = HashMultimap.create();
        for (final String layerSpec : Splitter.on(';').trimResults().omitEmptyStrings()
                .split(properties.getProperty(pr + "layers"))) {
            final int index = layerSpec.indexOf(':');
            final String layer = layerSpec.substring(0, index).trim();
            this.layers.add(layer);
            for (final String field : layerSpec.substring(index + 1).split("[//s,]+")) {
                this.layerFields.put(layer, field.trim());
            }
        }

        // Build the enricher
        this.enricher = Enricher.create(root, properties, "pikesir.enricher.");

        // Build the analyzer
        this.analyzer = Analyzer.create(root, properties, "pikesir.analyzer.");

        // Build the aggregator
        this.aggregator = Aggregator.create(root, properties, "pikesir.aggregator.");
    }

    public void enrichDocs() throws IOException {
        enrichHelper(this.pathDocsRDF, this.pathDocsRDFE, "Enriching documents");
    }

    public void enrichQueries() throws IOException {
        enrichHelper(this.pathQueriesRDF, this.pathQueriesRDFE, "Enriching queries");
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
                inTriples.addAndGet(model.size());
                this.enricher.enrich(model);
                outTriples.addAndGet(model.size());
                writeTriples(outputPath, model);
            } catch (final Throwable ex) {
                Throwables.propagate(ex);
            }
        });

        LOGGER.info("Done in {} ms ({} triples in, {} triples out)", System.currentTimeMillis()
                - ts, inTriples, outTriples);
    }

    public void analyzeDocs() throws IOException {
        analyzeHelper(this.pathDocsNAF, this.pathDocsRDFE, this.pathDocsTerms,
                "Analyzing documents", false);
    }

    public void analyzeQueries() throws IOException {
        analyzeHelper(this.pathQueriesNAF, this.pathQueriesRDFE, this.pathQueriesTerms,
                "Analyzing queries", true);
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
                } catch (final Throwable ex) {
                    Throwables.propagate(ex);
                }
            });
        }

        LOGGER.info("Done in {} ms ({} terms out)", System.currentTimeMillis() - ts, outTerms);
    }

    public void index() throws IOException {

        final long ts = System.currentTimeMillis();
        LOGGER.info("Building Lucene index");

        // Create index directory if necessary and wipe out existing directory contents
        initDir(this.pathIndex);

        // TODO: avoid loading everything into RAM; parallelize
        Map<String, TermVector> vectors = Maps.newHashMap();
        try (Reader reader = IO.utf8Reader(IO.buffer(IO.read(this.pathDocsTerms.toAbsolutePath()
                .toString())))) {
            vectors = TermVector.read(reader);
        }

        final FSDirectory indexDir = FSDirectory.open(this.pathIndex.toFile());
        final IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_10_3,
                new KeywordAnalyzer());

        int numTerms = 0;
        try (IndexWriter writer = new IndexWriter(indexDir, config)) {
            for (final Map.Entry<String, TermVector> entry : vectors.entrySet()) {
                final Document doc = new Document();
                doc.add(new TextField("id", entry.getKey(), Store.YES));
                for (final Term term : entry.getValue().getTerms()) {
                    for (int i = 0; i < (int) Math.ceil(term.getWeight()); ++i) {
                        doc.add(new TextField(term.getField().getID(), term.getValue(), Store.YES));
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
        LOGGER.info("Searching Lucene index");

        // Read relevances
        final Map<String, Map<String, Double>> rels = readRelevances(this.pathQueriesRelevances);

        // Read queries
        Map<String, TermVector> queries = Maps.newHashMap();
        try (Reader reader = IO.utf8Reader(IO.buffer(IO.read(this.pathQueriesTerms
                .toAbsolutePath().toString())))) {
            queries = TermVector.read(reader);
        }

        // Materialize all possible layer combinations and associated evaluators
        final Map<Set<String>, RankingScore.Evaluator> evaluators = Maps.newHashMap();
        for (final Set<String> combination : Sets.powerSet(ImmutableSet.copyOf(this.layers))) {
            if (!combination.isEmpty()) {
                evaluators.put(combination, RankingScore.evaluator(10));
            }
        }

        // Create results directory if necessary and wipe out existing content
        initDir(this.pathResults);

        // Open the index for read and evaluate all the queries in parallel
        try (IndexReader reader = DirectoryReader.open(FSDirectory.open(this.pathIndex.toFile()))) {
            final IndexSearcher searcher = new IndexSearcher(reader);
            final List<Runnable> queryJobs = Lists.newArrayList();
            for (final String queryID : Ordering.natural().sortedCopy(queries.keySet())) {
                final TermVector queryVector = queries.get(queryID);
                final Map<String, Double> queryRels = rels.get(queryID);
                queryJobs.add(new Runnable() {

                    @Override
                    public void run() {
                        searchHelper(searcher, queryID, queryVector, queryRels, evaluators);
                    }

                });
            }
            Environment.run(queryJobs);
        }

        // Write aggregate results
        try (Writer writer = IO.utf8Writer(IO.buffer(IO.write(this.pathResults
                .resolve("aggregates.tsv").toAbsolutePath().toString())))) {
            for (final Map.Entry<Set<String>, RankingScore.Evaluator> entry : evaluators
                    .entrySet()) {
                writer.append(Joiner.on(",").join(entry.getKey())).append("\t")
                .append(formatRankingScore(entry.getValue().get(), "\t")).append("\n");
            }
        }

        LOGGER.info("Done in {} ms", System.currentTimeMillis() - ts);
    }

    private void searchHelper(final IndexSearcher searcher, final String queryID,
            final TermVector queryVector, final Map<String, Double> rels,
            final Map<Set<String>, RankingScore.Evaluator> evaluators) {

        LOGGER.info("Evaluating query {}", queryID);

        // TODO
    }

    private static String formatRankingScore(final RankingScore score, final String separator) {
        final StringBuilder builder = new StringBuilder();
        builder.append(score.getPrecision(1)).append('\t');
        builder.append(score.getPrecision(3)).append('\t');
        builder.append(score.getPrecision(5)).append('\t');
        builder.append(score.getPrecision(10)).append('\t');
        builder.append(score.getMRR()).append('\t');
        builder.append(score.getNDCG()).append('\t');
        builder.append(score.getNDCG(10)).append('\t');
        builder.append(score.getMAP()).append('\t');
        builder.append(score.getMAP(10)).append('\t');
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
        final Tracker tracker = new Tracker(LOGGER, "Processing " + count + " files",
                "Processed %d files (%d files/s avg)",
                "Processed %d files (%d files/s, %d files/s avg)");

        tracker.start();
        try {
            Environment.run(files.<Runnable>transform((final File file) -> new Runnable() {

                @Override
                public void run() {
                    try {
                        consumer.accept(file.toPath());
                    } catch (final Throwable ex) {
                        throw new RuntimeException("Could not process " + file, ex);
                    } finally {
                        tracker.increment();
                    }
                }

            }));
        } finally {
            tracker.end();
        }
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

}
