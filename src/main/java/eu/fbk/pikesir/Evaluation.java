package eu.fbk.pikesir;

import java.io.IOException;
import java.io.Writer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.primitives.Doubles;

import org.apache.commons.math3.stat.inference.TTest;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.TopDocs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.pikesir.util.RankingScore;
import eu.fbk.pikesir.util.RankingScore.Measure;
import eu.fbk.rdfpro.util.Environment;
import eu.fbk.rdfpro.util.IO;

// Utility class used by PikesIR

final class Evaluation {

    private static final Logger LOGGER = LoggerFactory.getLogger(Evaluation.class);

    private static final Measure[] REPORTED_MEASURES = new Measure[] { Measure.P1, Measure.P3,
            Measure.P5, Measure.P10, Measure.MRR, Measure.NDCG, Measure.NDCG10, Measure.MAP,
            Measure.MAP10 };

    private final IndexSearcher searcher;

    private final Ranker ranker;

    private final List<String> layers;

    private final Measure sortMeasure;

    private final String[][] settings;

    private final int baselineIndex;

    public Evaluation(final IndexSearcher searcher, final Ranker ranker,
            final Iterable<String> layers, final Iterable<String> baselineLayers,
            final Measure sortMeasure) {

        final List<String> layerList = ImmutableList.copyOf(layers);
        final String[][] settings = new String[(1 << layerList.size()) - 1][];
        final Set<String> baselineSet = ImmutableSet.copyOf(baselineLayers);
        int baselineIndex = 0;
        for (int i = 0; i < settings.length; ++i) {
            int index = 0;
            settings[i] = new String[Integer.bitCount(i + 1)];
            for (int j = 0; j < layerList.size(); ++j) {
                if ((i + 1 & 1 << j) != 0) {
                    settings[i][index++] = layerList.get(j);
                }
            }
            if (baselineSet.size() == settings[i].length
                    && baselineSet.containsAll(Arrays.asList(settings[i]))) {
                baselineIndex = i;
            }
        }

        this.searcher = searcher;
        this.ranker = ranker;
        this.layers = layerList;
        this.sortMeasure = sortMeasure;
        this.settings = settings;
        this.baselineIndex = baselineIndex;
    }

    public void run(final Map<String, TermVector> queries,
            final Map<String, Map<String, Double>> rels, final Path resultPath) throws IOException {

        // Compute the statistics to be fed to ranker, based on supplied queries
        final Ranker.Statistics statistics = computeStatistics(queries.values());

        final List<QueryEvaluation> evaluations = evaluateQueries(queries, rels, statistics);

        final RankingScore[] scores = aggregateScores(evaluations);

        final Map<Measure, float[]> pvalues = pairedTTest(evaluations);

        // Write aggregate scores
        writeAggregateScores(resultPath, scores);

        // Write a TSV file for each setting, listing the results obtained for each query
        for (int i = 0; i < this.settings.length; ++i) {
            writeSettingScores(resultPath, evaluations, i);
        }

        // Write a TSV file for each query, listing the results obtained for each setting
        // (these files contain the same data of the ones before, just organized differently)
        for (final QueryEvaluation evaluation : evaluations) {
            writeQueryScores(resultPath, evaluation);
            writeQueryRankings(resultPath, evaluation);
        }

        // Report top aggregate scores
        logCompletion(scores, pvalues);
    }

    private Ranker.Statistics computeStatistics(final Iterable<TermVector> queryVectors)
            throws IOException {

        // Retrieve statistics for all the configured layers
        final Map<String, CollectionStatistics> layerStats = Maps.newHashMap();
        for (final String layer : this.layers) {
            layerStats.put(layer, this.searcher.collectionStatistics(layer));
        }

        // Retrieve statistics for all the terms appearing in queries
        final Map<Term, TermStatistics> termStats = Maps.newHashMap();
        for (final TermVector queryVector : queryVectors) {
            for (final Term term : queryVector.getTerms()) {
                if (!termStats.containsKey(term)) {
                    final String layer = term.getField();
                    final String value = term.getValue();
                    final org.apache.lucene.index.Term luceneTerm;
                    luceneTerm = new org.apache.lucene.index.Term(layer, value);
                    termStats.put(term, this.searcher.termStatistics(luceneTerm, //
                            TermContext.build(this.searcher.getTopReaderContext(), luceneTerm)));
                }
            }
        }

        // Build and return the Stats that will be supplied to the Ranker
        return new Ranker.Statistics(layerStats, termStats);
    }

    private List<QueryEvaluation> evaluateQueries(final Map<String, TermVector> queries,
            final Map<String, Map<String, Double>> rels, final Ranker.Statistics statistics)
            throws IOException {

        final List<QueryEvaluation> evaluations = Lists.newArrayList();
        final Map<Integer, String> docIDs = Maps.newConcurrentMap();
        final Map<String, TermVector> docVectors = Maps.newConcurrentMap();
        for (final String queryID : Ordering.natural().sortedCopy(queries.keySet())) {
            evaluations.add(new QueryEvaluation(queryID, queries.get(queryID), docIDs, docVectors,
                    rels.get(queryID), statistics));
        }
        Environment.run(evaluations);
        return evaluations;
    }

    private RankingScore[] aggregateScores(final List<QueryEvaluation> evaluations) {

        // Compute aggregated scores and sort them
        final RankingScore[] scores = new RankingScore[this.settings.length];
        for (int i = 0; i < this.settings.length; ++i) {
            final List<RankingScore> settingScores = Lists.newArrayList();
            for (final QueryEvaluation evaluation : evaluations) {
                settingScores.add(evaluation.scores[i]);
            }
            scores[i] = RankingScore.average(settingScores);
        }
        return scores;
    }

    private Map<Measure, float[]> pairedTTest(final List<QueryEvaluation> evaluations) {

        final Map<Measure, float[]> pvalues = Maps.newHashMap();

        final int numQueries = evaluations.size();
        for (final Measure measure : REPORTED_MEASURES) {
            final double[][] settingsValues = new double[this.settings.length][];
            for (int i = 0; i < this.settings.length; ++i) {
                final double[] values = new double[numQueries];
                for (int j = 0; j < numQueries; ++j) {
                    values[j] = evaluations.get(j).scores[i].get(measure);
                    settingsValues[i] = values;
                }
            }
            final float[] pvals = new float[this.settings.length];
            final double[] baselineValues = settingsValues[this.baselineIndex];

            for (int i = 0; i < this.settings.length; ++i) {
                final TTest test = new TTest();
                pvals[i] = (float) test.pairedTTest(settingsValues[i], baselineValues);
            }
            pvalues.put(measure, pvals);
        }

        return pvalues;
    }

    private void writeAggregateScores(final Path resultPath, final RankingScore[] scores)
            throws IOException {

        final RankingScore[] sortedScores = scores.clone();
        final int[] indexes = sort(sortedScores, RankingScore.comparator(this.sortMeasure, true));

        try (Writer writer = IO.utf8Writer(IO.buffer(IO.write(resultPath.resolve("aggregates.csv")
                .toAbsolutePath().toString())))) {
            writer.append("layers;p@1;p@3;p@5;p@10;mrr;ndcg;ndcg@10;map;map@10\n");
            for (int i = 0; i < sortedScores.length; ++i) {
                writer.append(Joiner.on(",").join(this.settings[indexes[i]])).append(";")
                        .append(formatRankingScore(sortedScores[i], ";")).append("\n");
            }
        }
    }

    private void writeQueryScores(final Path resultPath, final QueryEvaluation evaluation)
            throws IOException {

        // Build a map from ranking scores to the setting index
        final Map<RankingScore, Integer> map = Maps.newIdentityHashMap();
        for (int i = 0; i < this.settings.length; ++i) {
            map.put(evaluation.scores[i], i);
        }

        // Write file, sorting scores from best to worst and using map to obtain settings & hits
        try (Writer writer = IO.utf8Writer(IO.buffer(IO.write(resultPath
                .resolve(evaluation.queryID + ".csv").toAbsolutePath().toString())))) {
            writer.append("layers;p@1;p@3;p@5;p@10;mrr;ndcg;ndcg@10;map;map@10;"
                    + "ranking (top 10)\n");
            for (final RankingScore score : RankingScore.comparator(this.sortMeasure, true)
                    .sortedCopy(Arrays.asList(evaluation.scores))) {
                final int index = map.get(score);
                writer.append(Joiner.on(',').join(this.settings[index]));
                writer.append(';');
                writer.append(formatRankingScore(score, ";"));
                writer.append(';');
                writer.append(Joiner.on(",").join(
                        Iterables.limit(Arrays.asList(evaluation.hits[index]), 10)));
                writer.append('\n');
            }
        }
    }

    private void writeQueryRankings(final Path resultPath, final QueryEvaluation evaluation)
            throws IOException {

        // Identify layers, associated documents and max number of documents
        int numHits = 0;
        final List<String> layers = Lists.newArrayList();
        final List<Hit[]> sortedHits = Lists.newArrayList();
        for (int i = 0; i < Evaluation.this.settings.length; ++i) {
            if (Evaluation.this.settings[i].length == 1) {
                layers.add(Evaluation.this.settings[i][0]);
                sortedHits.add(evaluation.hits[i]);
                numHits = Math.max(numHits, evaluation.hits[i].length);
            }
        }

        // Write file
        try (Writer writer = IO.utf8Writer(IO.buffer(IO.write(resultPath
                .resolve(evaluation.queryID + "-rank.csv").toAbsolutePath().toString())))) {

            // Write header
            for (final String layer : layers) {
                writer.append(layer).append(";;;");
            }
            writer.append(";;\n");

            // Write rows
            final int numLayers = layers.size();
            final int numRows = Math.min(50, numHits);
            final String[] rowIDs = new String[numLayers];
            final double[] rowScores = new double[numLayers];
            for (int i = 0; i < numRows; ++i) {
                for (int j = 0; j < numLayers; ++j) {
                    final Hit[] hits = sortedHits.get(j);
                    final Hit hit = i < hits.length ? sortedHits.get(j)[i] : null;
                    rowIDs[j] = hit == null ? null : hit.documentID;
                    rowScores[j] = hit == null ? 0.0 : hit.score;
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
                        final Double rel = evaluation.rels.get(id);
                        writer.append(id).append(rel == null ? "" : " (" + rel + ")").append(';')
                                .append(Double.toString(score)).append(";;");
                    }
                }
                writer.append("\n");
            }
        }
    }

    private void writeSettingScores(final Path resultPath,
            final Iterable<QueryEvaluation> evaluations, final int settingIndex)
            throws IOException {

        try (Writer writer = IO.utf8Writer(IO.buffer(IO.write(resultPath
                .resolve("setting-" + Joiner.on('-').join(this.settings[settingIndex]) + ".csv")
                .toAbsolutePath().toString())))) {
            writer.append("query;p@1;p@3;p@5;p@10;mrr;ndcg;ndcg@10;map;map@10\n");
            for (final QueryEvaluation evaluation : evaluations) {
                writer.append(evaluation.queryID).append(';')
                        .append(formatRankingScore(evaluation.scores[settingIndex], ";"))
                        .append('\n');
            }
        }
    }

    private void logCompletion(final RankingScore[] scores, final Map<Measure, float[]> pvalues) {

        final RankingScore[] sortedScores = scores.clone();
        final int[] indexes = sort(sortedScores, RankingScore.comparator(this.sortMeasure, true));

        if (LOGGER.isInfoEnabled()) {
            final StringBuilder builder = new StringBuilder("Top scores:");
            for (int i = 0; i < Math.min(100, sortedScores.length); ++i) {
                final RankingScore score = sortedScores[i];
                builder.append(String.format("\n  %-40s",
                        Joiner.on(',').join(this.settings[indexes[i]])));
                for (final Measure measure : REPORTED_MEASURES) {
                    builder.append(String.format(" %s=%.3f/", measure.toString(),
                            score.get(measure)));
                    if (indexes[i] == this.baselineIndex) {
                        builder.append("-----");
                    } else {
                        builder.append(String.format("%-5.3f", pvalues.get(measure)[indexes[i]]));
                    }
                }
            }
            LOGGER.info(builder.toString());
        }
    }

    private static <T> int[] sort(final T[] array, final Comparator<? super T> comparator) {
        final Map<T, Integer> map = Maps.newHashMap();
        for (int i = 0; i < array.length; ++i) {
            map.put(array[i], i);
        }
        Arrays.sort(array, comparator);
        final int[] indexes = new int[array.length];
        for (int i = 0; i < array.length; ++i) {
            indexes[i] = map.get(array[i]);
        }
        return indexes;
    }

    private static TermVector toTermVector(final Document document) {
        final TermVector.Builder builder = TermVector.builder();
        for (final IndexableField field : document.getFields()) {
            final String name = field.name();
            if (!"id".equals(name)) {
                final String value = field.stringValue();
                try {
                    builder.addTerm(name, value);
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

    private final class QueryEvaluation implements Runnable {

        // Input

        final String queryID;

        final TermVector queryVector;

        final Map<Integer, String> cachedDocumentIDs;

        final Map<String, TermVector> cachedDocumentVectors;

        final Map<String, Double> rels;

        final Ranker.Statistics statistics;

        // Output

        final Hit[][] hits; // a Hit[] for each setting

        final RankingScore[] scores; // a RankingScore for each setting

        QueryEvaluation(final String queryID, final TermVector queryVector,
                final Map<Integer, String> cachedDocumentIDs,
                final Map<String, TermVector> cachedDocumentVectors,
                final Map<String, Double> rels, final Ranker.Statistics statistics) {

            this.queryID = queryID;
            this.queryVector = queryVector;
            this.cachedDocumentIDs = cachedDocumentIDs;
            this.cachedDocumentVectors = cachedDocumentVectors;
            this.statistics = statistics;
            this.rels = rels;
            this.hits = new Hit[Evaluation.this.settings.length][];
            this.scores = new RankingScore[Evaluation.this.settings.length];
        }

        @Override
        public void run() {

            try {
                // Identify matching documents using boolean model (delegated to Lucene, OR semantics)
                final Multimap<String, String> matches = matchDocuments();

                // Compute and evaluate rankings for each setting
                rankDocuments(matches);

                // Log number of hits and best ranking, if enabled
                logCompletion();

            } catch (final Throwable ex) {
                // Wrap and propagate
                throw Throwables.propagate(ex);
            }
        }

        private Multimap<String, String> matchDocuments() throws IOException, ParseException {

            // Evaluate a Lucene query for each layer and populate a multimap with matched document IDs
            final Multimap<String, String> matches = HashMultimap.create();
            for (final String layer : Evaluation.this.layers) {

                // Compose the query string
                final StringBuilder builder = new StringBuilder();
                String separator = "";
                for (final Term term : this.queryVector.getTerms(layer)) {
                    builder.append(separator);
                    builder.append(layer).append(":\"").append(term.getValue()).append("\"");
                    separator = " OR ";
                }
                final String queryString = builder.toString();

                // Skip evaluation if the query is empty
                if (queryString.isEmpty()) {
                    continue;
                }

                // Evaluate the query
                final QueryParser parser = new QueryParser("default-field", new KeywordAnalyzer());
                final Query query = parser.parse(queryString);
                final TopDocs results = Evaluation.this.searcher.search(query, 1000);
                LOGGER.debug("{} results obtained from query {}", results.scoreDocs.length,
                        queryString);

                // Populate the matches multimap. This requires mapping the numerical doc ID to
                // the corresponding String one. We also retrieve the associated Lucene document
                // and cache the document term vector for later reuse.
                for (final ScoreDoc scoreDoc : results.scoreDocs) {
                    String docID;
                    synchronized (this.cachedDocumentIDs) {
                        docID = this.cachedDocumentIDs.get(scoreDoc.doc);
                        if (docID == null) {
                            final Document doc = Evaluation.this.searcher.doc(scoreDoc.doc);
                            docID = doc.get("id");
                            this.cachedDocumentIDs.put(scoreDoc.doc, docID);
                            this.cachedDocumentVectors.put(docID, toTermVector(doc));
                        }
                    }
                    matches.put(layer, docID);
                }
            }
            return matches;
        }

        private void rankDocuments(final Multimap<String, String> matches) {

            // Compute and evaluate a ranking for each considered setting
            for (int i = 0; i < Evaluation.this.settings.length; ++i) {

                // Retrieve the current setting
                final String[] setting = Evaluation.this.settings[i];

                // Obtain all the IDs of matching document for this setting
                final Set<String> documentIDs = Sets.newLinkedHashSet();
                for (final String layer : setting) {
                    documentIDs.addAll(matches.get(layer));
                }

                if (documentIDs.isEmpty()) {
                    // Update ranking scores by comparing an empty answer with gold relevances
                    this.hits[i] = new Hit[0];
                    this.scores[i] = RankingScore.evaluator(10) //
                            .add(ImmutableList.of(), this.rels).get();

                } else {
                    // Apply the ranker to compute a score for each matched document
                    final String[] ids = documentIDs.toArray(new String[documentIDs.size()]);
                    final TermVector[] vectors = new TermVector[ids.length];
                    for (int j = 0; j < ids.length; ++j) {
                        vectors[j] = this.cachedDocumentVectors.get(ids[j]);
                    }
                    final float[] scores = Evaluation.this.ranker.rank(
                            this.queryVector.project(Arrays.asList(setting)), vectors,
                            this.statistics);

                    // Build and store a sorted list of Hit objects
                    final Hit[] hits = new Hit[ids.length];
                    for (int j = 0; j < ids.length; ++j) {
                        hits[j] = new Hit(ids[j], scores[j]);
                    }
                    Arrays.sort(hits);
                    this.hits[i] = hits;

                    // Update ranking scores based on the obtained Hit list
                    final String[] idsSorted = new String[ids.length];
                    final Map<String, Double> scoresMap = Maps.newHashMap();
                    for (int j = 0; j < hits.length; ++j) {
                        idsSorted[j] = hits[j].documentID;
                        scoresMap.put(hits[j].documentID, (double) hits[j].score);
                    }
                    this.scores[i] = RankingScore.evaluator(10)
                            .add(Arrays.asList(idsSorted), this.rels).get();
                }
            }
        }

        private void logCompletion() {

            if (LOGGER.isInfoEnabled()) {
                int bestIndex = -1;
                int numHits = 0;
                for (int i = 0; i < Evaluation.this.settings.length; ++i) {
                    numHits = Math.max(numHits, this.hits[i].length);
                    if (bestIndex < 0
                            || RankingScore.comparator(Evaluation.this.sortMeasure, true).compare(
                                    this.scores[i], this.scores[bestIndex]) <= 0) {
                        bestIndex = i;
                    }
                }
                LOGGER.info("Evaluated {} - {} hits, best: {} {}", this.queryID, String.format(
                        "%4d", numHits), this.scores[bestIndex],
                        Joiner.on(',').join(Evaluation.this.settings[bestIndex]));
            }
        }

    }

    private static final class Hit implements Comparable<Hit> {

        final String documentID;

        final float score;

        Hit(final String documentID, final float score) {
            this.documentID = Objects.requireNonNull(documentID);
            this.score = score;
        }

        @Override
        public int compareTo(final Hit other) {
            int result = Double.compare(other.score, this.score);
            if (result == 0) {
                result = this.documentID.compareTo(other.documentID);
            }
            return result;
        }

        @Override
        public boolean equals(final Object object) {
            if (object == this) {
                return true;
            }
            if (!(object instanceof Hit)) {
                return false;
            }
            final Hit other = (Hit) object;
            return this.documentID.equals(other.documentID);
        }

        @Override
        public int hashCode() {
            return this.documentID.hashCode();
        }

        @Override
        public String toString() {
            final StringBuilder builder = new StringBuilder();
            builder.append(this.documentID);
            builder.append(':');
            builder.append(String.format("%.3f", this.score));
            return builder.toString();
        }

    }

}