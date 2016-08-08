package eu.fbk.ke4ir;

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
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.AbstractMap.SimpleEntry;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.*;
import com.google.common.io.ByteStreams;

import com.sun.tools.javac.util.Pair;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.rio.RDFHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ixa.kaflib.KAFDocument;

import eu.fbk.ke4ir.util.CommandLine;
import eu.fbk.ke4ir.util.RankingScore;
import eu.fbk.rdfpro.AbstractRDFHandlerWrapper;
import eu.fbk.rdfpro.RDFHandlers;
import eu.fbk.rdfpro.RDFSources;
import eu.fbk.rdfpro.util.Environment;
import eu.fbk.rdfpro.util.IO;
import eu.fbk.rdfpro.util.QuadModel;
import eu.fbk.rdfpro.util.Statements;

public class KE4IR {

    private static final Logger LOGGER = LoggerFactory.getLogger(KE4IR.class);

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

    private final Set<String> evalBaseline;

    private final RankingScore.Measure evalSortMeasure;

    private final String evalStatisticalTest;

    private final Enricher enricher;

    private final Analyzer analyzer;

    private final Ranker ranker;

    public static void main(final String... args) {

        try {
            // Parse command line
            final CommandLine cmd = CommandLine
                    .parser()
                    .withName("ke4ir-eval")
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
                    .withOption("r", "rerank", "dump 4 rerank")
                    .withHeader("supports all the operations involved in the evaluation of " //
                            + "semantic information retrieval: enrichment, analysis, " //
                            + "indexing, search").parse(args);

            // Extract options
            final Path propertiesPath = Paths.get(cmd.getOptionValue("p", String.class,
                    System.getProperty("user.dir") + "/ke4ir.properties"));
            boolean enrichDocs = cmd.hasOption("enrich-docs") || cmd.hasOption("e");
            boolean enrichQueries = cmd.hasOption("enrich-queries") || cmd.hasOption("e");
            boolean analyzeDocs = cmd.hasOption("analyze-docs") || cmd.hasOption("a");
            boolean analyzeQueries = cmd.hasOption("analyze-queries") || cmd.hasOption("a");
            boolean index = cmd.hasOption("i");
            boolean search = cmd.hasOption("s");
            boolean rerank = cmd.hasOption("r");

            // Abort if properties file does not exist
            if (!Files.exists(propertiesPath)) {
                throw new CommandLine.Exception("Properties file '" + propertiesPath
                        + "' does not exist");
            }

            // Read properties
            final long ts = System.currentTimeMillis();
            final Properties properties = new Properties();
            try (Reader reader = IO.utf8Reader(IO.buffer(IO.read(propertiesPath.toString())))) {
                properties.load(reader);
            }

            // Force certain actions if specified in properties file
            final String pr = "ke4ir.forcecmd.";
            enrichDocs |= Boolean.parseBoolean(properties.getProperty(pr + "enrichdocs", "false"));
            enrichQueries |= Boolean.parseBoolean(properties.getProperty( //
                    pr + "enrichqueries", "false"));
            analyzeDocs |= Boolean.parseBoolean(properties.getProperty( //
                    pr + "analyzedocs", "false"));
            analyzeQueries |= Boolean.parseBoolean(properties.getProperty( //
                    pr + "analyzequeries", "false"));
            index |= Boolean.parseBoolean(properties.getProperty(pr + "index", "false"));
            search |= Boolean.parseBoolean(properties.getProperty(pr + "search", "false"));
            rerank |= Boolean.parseBoolean(properties.getProperty(pr + "rerank", "false"));

            //            final List<String> data = Lists.newArrayList();
            //            for (int j = 0; j <= 100; j += 1) {
            //                final double w = j * 0.01;
            //                final String ws = "textual:" + (1 - w) + " uri:" + w / 4 + " type:" + w / 4
            //                        + " frame:" + w / 4 + " time:" + w / 4;
            //                System.out.println("\n\n\n**** " + ws + " ****\n\n");
            //                properties.setProperty("ke4ir.ranker.tfidf.weights", ws);

            // Initialize the KE4IR main object
            final KE4IR ke4ir = new KE4IR(propertiesPath.getParent(), properties, "ke4ir.");
            LOGGER.info("Initialized in {} ms", System.currentTimeMillis() - ts);

            // Perform the requested operations
            if (enrichQueries) {
                ke4ir.enrichQueries();
            }
            if (enrichDocs) {
                ke4ir.enrichDocs();
            }
            if (analyzeQueries) {
                ke4ir.analyzeQueries();
            }
            if (analyzeDocs) {
                ke4ir.analyzeDocs();
            }
            if (index) {
                ke4ir.index();
            }
            if (search) {
                ke4ir.search();
            }
            if (rerank) {
                ke4ir.dump4Reranker();
            }

            //                final List<String> lines = Files.readAllLines(propertiesPath.getParent().resolve(
            //                        "results/aggregates.csv"));
            //                for (int i = 1; i < lines.size(); ++i) {
            //                    data.add(String.format("%.2f", w) + ";" + lines.get(i));
            //                }
            //            }
            //            Files.write(propertiesPath.getParent().resolve("results/experiment.csv"), data,
            //                    Charsets.UTF_8);

        } catch (final Throwable ex) {
            // Display error information and terminate
            CommandLine.fail(ex);
        }
    }

    public KE4IR(final Path root, final Properties properties, final String prefix) {

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
        this.layers = Splitter.on(Pattern.compile("[\\s,;]+")).trimResults().omitEmptyStrings()
                .splitToList(properties.getProperty(pr + "layers"));

        // Retrieve evaluation settings
        this.evalSortMeasure = RankingScore.Measure.create(properties.getProperty(
                pr + "results.sort", "map").trim());
        this.evalBaseline = ImmutableSet.copyOf(properties.getProperty(pr + "results.baseline",
                "textual").split("\\s+"));
        this.evalStatisticalTest = properties.getProperty(pr + "results.test", "ttest").trim()
                .toLowerCase();

        // Build the enricher
        this.enricher = Enricher.create(root, properties, "ke4ir.enricher.");

        // Build the analyzer
        this.analyzer = Analyzer.create(root, properties, "ke4ir.analyzer.");

        // Build the ranker
        this.ranker = Ranker.create(root, properties, "ke4ir.ranker.");

        // Report configuration
        LOGGER.info("Using enricher: {}", this.enricher);
        LOGGER.info("Using analyzer: {}", this.analyzer);
        LOGGER.info("Using ranker: {}", this.ranker);
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
                LOGGER.info("Enriched {} - {} triples obtained from {} triples", path,
                        model.size(), sizeBefore);
            } catch (final Throwable ex) {
                Throwables.propagate(ex);
            }
        });

        LOGGER.info("Done in {} ms ({} triples in, {} triples out)", System.currentTimeMillis()
                - ts, inTriples, outTriples);
    }

    public void analyzeDocs() throws IOException {
        analyzeHelper(this.pathDocsNAF, this.pathDocsRDFE, this.pathDocsTerms,
                "=== Analyzing documents ===");
    }

    public void analyzeQueries() throws IOException {
        analyzeHelper(this.pathQueriesNAF, this.pathQueriesRDFE, this.pathQueriesTerms,
                "=== Analyzing queries ===");
    }

    private void analyzeHelper(final Path pathNAF, final Path pathRDFE, final Path pathTerms,
            final String message) throws IOException {

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
                    String id = document.getPublic().publicId;
                    if (id == null) {
                        URI uri = new URIImpl(document.getPublic().uri);
                        id = uri.getLocalName();
                    }
                    final TermVector.Builder builder = TermVector.builder();
                    this.analyzer.analyze(document, model, builder);
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

        final FSDirectory indexDir = FSDirectory.open(this.pathIndex);
        final IndexWriterConfig config = new IndexWriterConfig(new KeywordAnalyzer());
        config.setSimilarity(FakeSimilarity.INSTANCE);

        int numTerms = 0;
        try (IndexWriter writer = new IndexWriter(indexDir, config)) {
            for (final Map.Entry<String, TermVector> entry : vectors.entrySet()) {

                final String docID = entry.getKey();
                final TermVector docVector = entry.getValue();

                LOGGER.info("Indexing {} - {} terms", docID, entry.getValue().size());

                final Document doc = new Document();
                doc.add(new TextField("id", docID, Store.YES));
                for (final Term term : docVector.getTerms()) {
                    for (int i = 0; i < term.getFrequency(); ++i) {
                        doc.add(new TextField(term.getField(), term.getValue(), Store.YES));
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
        final Map<String, TermVector> queries = readQueries(this.pathQueriesTerms);

        // Read document vectors // TODO: this data should be loaded from the index on demand!
        Map<String, TermVector> documents;
        try (Reader reader = IO.utf8Reader(IO.buffer(IO.read(this.pathDocsTerms.toAbsolutePath()
                .toString())))) {
            documents = TermVector.read(reader);
        }

        // Create results directory if necessary and wipe out existing content
        initDir(this.pathResults);

        try (IndexReader reader = DirectoryReader.open(FSDirectory.open(this.pathIndex))) {
            final IndexSearcher searcher = new IndexSearcher(reader);
            searcher.setSimilarity(FakeSimilarity.INSTANCE);
            new Evaluation(searcher, this.ranker, this.layers, this.evalBaseline,
                    this.evalSortMeasure, this.evalStatisticalTest, documents).run(queries, rels,
                    this.pathResults);
        }

        LOGGER.info("Done in {} ms", System.currentTimeMillis() - ts);
    }



    public void dump4Reranker() throws IOException, ParseException {

        final long ts = System.currentTimeMillis();
        LOGGER.info("=== Dump 4 Rerank Lucene index ===");


        List<SimpleEntry<String,String>> features = new ArrayList<>();
        features.add(new SimpleEntry<String,String>("textual", "tdf"));
        features.add(new SimpleEntry<String,String>("time", "tdf"));
        features.add(new SimpleEntry<String,String>("type", "tdf"));
        features.add(new SimpleEntry<String,String>("frame", "tdf"));
        features.add(new SimpleEntry<String,String>("uri", "tdf"));

        features.add(new SimpleEntry<String,String>("textual", "idf"));
        features.add(new SimpleEntry<String,String>("time", "idf"));
        features.add(new SimpleEntry<String,String>("type", "idf"));
        features.add(new SimpleEntry<String,String>("frame", "idf"));
        features.add(new SimpleEntry<String,String>("uri", "idf"));

        features.add(new SimpleEntry<String,String>("textual", "sim"));
        features.add(new SimpleEntry<String,String>("time", "sim"));
        features.add(new SimpleEntry<String,String>("type", "sim"));
        features.add(new SimpleEntry<String,String>("frame", "sim"));
        features.add(new SimpleEntry<String,String>("uri", "sim"));


        // Read relevances
        final Map<String, Map<String, Double>> rels = readRelevances(this.pathQueriesRelevances);

        // Read queries
        final Map<String, TermVector> queries = readQueries(this.pathQueriesTerms);

        // Read document vectors // TODO: this data should be loaded from the index on demand!
        Map<String, TermVector> documents;
        try (Reader reader = IO.utf8Reader(IO.buffer(IO.read(this.pathDocsTerms.toAbsolutePath()
                .toString())))) {
            documents = TermVector.read(reader);
        }

        // Create results directory if necessary and wipe out existing content
        //initDir(this.pathResults);

        try (IndexReader reader = DirectoryReader.open(FSDirectory.open(this.pathIndex))) {
            final IndexSearcher searcher = new IndexSearcher(reader);
            searcher.setSimilarity(FakeSimilarity.INSTANCE);
            //todo

            final Map<String, Map<String, Map<String,Map<String, Double>>>> rankmap = Maps.newHashMap(); //<query, <doc, <layer,value>>>


            for (Map.Entry<String,TermVector> queryEntry: queries.entrySet()){
                String qid = queryEntry.getKey();
                TermVector qv = queryEntry.getValue();

                List<String> docs = matchDocuments(searcher,qv);
//                for (String ID:rels.get(qid).keySet()){
//                    if (!docs.contains(ID)) docs.add(ID);
//                }

                final Map<String, Map<String,Map<String, Double>>> q_map = Maps.newHashMap(); //<doc, <layer,value>>

                for (String did:docs){
                    TermVector dv = documents.get(did);

                    // Allocate the array of document scores, one element for each document vector
                    //final float[] scores = new float[docVectors.length];

                    // We compute the score of each document by iterating first on query terms (so that
                    // some state can be saved and reused) and then on document vectors. The relative
                    // order of the two iterations is irrelevant.


                    final Map<String,Map<String, Double>> qd_map = Maps.newHashMap(); //<layer,<type,value>>
//                    final Map<String, Double> idf_qd_map = Maps.newHashMap();
//                    final Map<String, Double> sim_qd_map = Maps.newHashMap();




                    for (final Term queryTerm : qv.getTerms()) {

                        // Retrieve the number of documents from Lucene statistics
                        final long numDocs = searcher.collectionStatistics(queryTerm.getField()).docCount();

                        // Extract term layer and associated weight.
                        final String layer = queryTerm.getField();
                        //final float weight = weights.getOrDefault(layer, 0.0f);

                        // Extract the document frequency (# documents having that term in the index)
                        TermStatistics stats = getTermStatistics(queryTerm,searcher);

                        final long docFreq = stats.docFreq();

                        // Skip in case the document does not contain the query term
                        final Term docTerm = dv.getTerm(layer, queryTerm.getValue());
                        if (docTerm == null) {
                            continue;
                        }

                        // Extract required frequencies
                        final double rfd = docTerm.getFrequency(); // raw frequency, document side
                        final double nfq = queryTerm.getWeight(); // normalized frequency, query side

                        // Compute TF / IDF
                        final float tfd = (float) (1.0 + Math.log(rfd)); // TF, document side
                        final float tfq = (float) nfq; // TF, query side
                        final float idf = (float) Math.log(numDocs / (double) docFreq); // IDF

                        // Update the document score
                        //scores[i] += tfd * idf * idf * tfd * weight;
                        //qid, did, layer, value....

                        //empty layer
                        if (!qd_map.containsKey(layer)){
                            Map<String, Double> qd_map_entry = Maps.newHashMap();
                            qd_map.put(layer,qd_map_entry);
                        }
                        Map<String, Double> qd_map_entry = qd_map.get(layer);

                        double tdfscore=0;
                        if (qd_map_entry.containsKey("tdf")){
                            tdfscore=qd_map_entry.get("tdf");
                        }
                        tdfscore += tfd * tfq;
                        qd_map_entry.put("tdf",tdfscore);

                        double idfscore=0;
                        if (qd_map_entry.containsKey("idf")){
                            idfscore=qd_map_entry.get("idf");
                        }
                        idfscore += idf * idf;
                        qd_map_entry.put("idf",idfscore);

                        double simscore=0;
                        if (qd_map_entry.containsKey("sim")){
                            simscore=qd_map_entry.get("sim");
                        }
                        simscore += tfd * tfq * idf * idf;
                        qd_map_entry.put("sim",simscore);
                        qd_map.put(layer,qd_map_entry);

                    }

                    q_map.put(did,qd_map);


                }
                rankmap.put(qid,q_map);
            }




            try (Writer writer = IO.utf8Writer(IO.buffer(IO.write(this.pathResults
                    .resolve("reranker.txt")
                    .toAbsolutePath().toString())))) {


                String header = "# COLUMN EXPLANATION: rel qid ";
                for (int i = 0; i < features.size(); i++) {
                    double measure = 0.0;
                    String layer = features.get(i).getKey();
                    String mes4layer = features.get(i).getValue();
                    header += "(" + layer + "," + mes4layer + ") ";
                }
                header += "# qid did ";
                writer.append(header).append('\n');


                Stream<Map.Entry<String, Map<String, Map<String, Map<String, Double>>>>> sortedRankmap = rankmap.entrySet().stream();
                sortedRankmap.sorted(Map.Entry.comparingByKey())
                        .forEachOrdered(q -> {

                            String qid = q.getKey();
                            //System.out.println(qid);
                            Stream<Map.Entry<String, Map<String, Map<String, Double>>>> sortedDoc = q.getValue().entrySet().stream();
                            sortedDoc.sorted(Map.Entry.comparingByKey())
                                    .forEachOrdered(d -> {
                                        String did = d.getKey();
                                        //System.out.println(did);
                                        String line = "";

                                        //print relevance value
                                        int rel = 0;
                                        if (rels.containsKey(qid))
                                            if (rels.get(qid).containsKey(did))
                                                //rel = rels.get(qid).get(did).intValue()-1;
                                                rel = rels.get(qid).get(did).intValue();
                                        line += rel + " ";

                                        //print qid
                                        line += qid.replace("q", "qid:") + " ";


                                        //print features

                                        for (int i = 0; i < features.size(); i++) {
                                            double measure = 0.0;
                                            String layer = features.get(i).getKey();
                                            String mes4layer = features.get(i).getValue();

                                            if (d.getValue().containsKey(layer)) {
                                                if (d.getValue().get(layer).containsKey(mes4layer)) {
                                                    measure = d.getValue().get(layer).get(mes4layer);
                                                }
                                            }
                                            line += (i + 1) + ":" + measure + " ";
                                        }

//                                    int i=1;
//                                    SortedSet<String> layers = new TreeSet<String>(d.getValue().keySet());
//
//                                    for (String layer:layers){
//                                        System.out.println(layer);
//                                        SortedSet<String> measures = new TreeSet<String>(d.getValue().get(layer).keySet());
//                                        for (String measure:measures) {
//                                            System.out.println(measure);
//                                            line += i++ + ":" + d.getValue().get(layer).get(measure) + " ";
//                                        }
//                                    }

                                        //print comment end of line
                                        line += "# " + qid + " " + did;

                                        //print line
                                        //System.out.println(line);
                                        try {
                                            writer.append(line).append('\n');
                                        } catch (IOException e) {
                                            e.printStackTrace();
                                        }
                                    });

                        });


            }


            try (Writer writer = IO.utf8Writer(IO.buffer(IO.write(this.pathResults
                    .resolve("qrels_trec_style.txt")
                    .toAbsolutePath().toString())))) {


                Stream<Map.Entry<String, Map<String, Double>>> sortedRels = rels.entrySet().stream();
                sortedRels.sorted(Map.Entry.comparingByKey())
                        .forEachOrdered(q -> {

                            String qid = q.getKey();
                            Stream<Map.Entry<String, Double>> sortedDoc = q.getValue().entrySet().stream();
                            sortedDoc.sorted(Map.Entry.comparingByKey())
                                    .forEachOrdered( d -> {
                                int rel=d.getValue().intValue();
                                if (rel>0) {
                                    String did = d.getKey();
                                    try {
                                        writer.append(qid.replace("q","")+" 0 "+did+" "+rel+"\n");
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                }

                            });

                        });


            }


            //print
            //for (String qid:){





        }

        LOGGER.info("Done in {} ms", System.currentTimeMillis() - ts);
    }



    private static List<String> matchDocuments(IndexSearcher searcher, TermVector queryVector) throws IOException, ParseException {

            // Compose the query string
            final StringBuilder builder = new StringBuilder();
            String separator = "";
            for (final Term term : queryVector.getTerms()) {
                builder.append(separator);
                builder.append(term.getField()).append(":\"").append(term.getValue()).append("\"");
                separator = " OR ";
            }
            final String queryString = builder.toString();

//            // Skip evaluation if the query is empty
//            if (queryString.isEmpty()) {
//                continue;
//            }

            // Evaluate the query
            final QueryParser parser = new QueryParser("default-field", new KeywordAnalyzer());
            final Query query = parser.parse(queryString);
            final TopDocs results = searcher.search(query, 1000);
            LOGGER.debug("{} results obtained from query {}", results.scoreDocs.length,
                    queryString);

            List<String> docs = new ArrayList<String>();


        // Populate the matches multimap. This requires mapping the numerical doc ID to
            // the corresponding String one. We also retrieve the associated Lucene document
            // and cache the document term vector for later reuse.
            for (final ScoreDoc scoreDoc : results.scoreDocs) {
                final Document doc = searcher.doc(scoreDoc.doc,
                                ImmutableSet.of("id"));
                docs.add(doc.get("id"));
            }

        return docs;
    }




    private static TermStatistics getTermStatistics (Term term,IndexSearcher searcher) throws IOException {

                    final String layer = term.getField();
                    final String value = term.getValue();
                    final org.apache.lucene.index.Term luceneTerm;
                    luceneTerm = new org.apache.lucene.index.Term(layer, value);
                    return searcher.termStatistics(luceneTerm, //
                           TermContext.build(searcher.getTopReaderContext(), luceneTerm));
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

    private static Map<String, TermVector> readQueries(final Path path) throws IOException {
        Map<String, TermVector> queries = Maps.newHashMap();
        try (Reader reader = IO.utf8Reader(IO.buffer(IO.read(path.toAbsolutePath().toString())))) {
            queries = TermVector.read(reader);
        }
        return queries;
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

    private static final class FakeSimilarity extends Similarity {

        public static final FakeSimilarity INSTANCE = new FakeSimilarity();

        @Override
        public long computeNorm(final FieldInvertState state) {
            return 0;
        }

        @Override
        public SimWeight computeWeight(final CollectionStatistics collectionStats,
                final TermStatistics... termStats) {
            return Weight.INSTANCE;
        }

        @Override
        public SimScorer simScorer(final SimWeight weight, final LeafReaderContext context)
                throws IOException {
            return Scorer.INSTANCE;
        }

        private static final class Weight extends Similarity.SimWeight {

            public static final Weight INSTANCE = new Weight();

            @Override
            public float getValueForNormalization() {
                return 1.0f;
            }

            @Override
            public void normalize(final float queryNorm, final float boost) {
            }

        }

        private static final class Scorer extends SimScorer {

            public static final Scorer INSTANCE = new Scorer();

            @Override
            public float score(final int doc, final float freq) {
                return 1.0f;
            }

            @Override
            public Explanation explain(final int doc, final Explanation freq) {
                return null;
            }

            @Override
            public float computeSlopFactor(final int distance) {
                return 1.0f;
            }

            @Override
            public float computePayloadFactor(final int doc, final int start, final int end,
                    final BytesRef payload) {
                return 1.0f;
            }

        }

    }

}
