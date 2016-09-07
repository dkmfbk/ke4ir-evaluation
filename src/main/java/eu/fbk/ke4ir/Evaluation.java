package eu.fbk.ke4ir;

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
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
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

import eu.fbk.ke4ir.util.ApproximateRandomization;
import eu.fbk.ke4ir.util.RankingScore;
import eu.fbk.ke4ir.util.RankingScore.Measure;
import eu.fbk.rdfpro.util.Environment;
import eu.fbk.rdfpro.util.IO;

// Utility class used by KE4IR

final class Evaluation {

    private static final Logger LOGGER = LoggerFactory.getLogger(Evaluation.class);

    private static final Measure[] REPORTED_MEASURES = new Measure[] { Measure.P1, Measure.P3,
            Measure.P5, Measure.P10, Measure.MRR, Measure.NDCG, Measure.NDCG10, Measure.MAP,
            Measure.MAP10 };

    private final IndexSearcher searcher;

    private final Ranker ranker;

    private final List<String> layers;

    private final Measure sortMeasure;

    private final String statisticalTest;

    private final String[][] settings;

    private final int baselineIndex;

    private final Integer maxDocs;

    public Evaluation(final IndexSearcher searcher, final Integer maxDocs, final Ranker ranker,
            final Iterable<String> layers, final Iterable<String> baselineLayers,
            final Measure sortMeasure, final String statisticalTest) {

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
        this.statisticalTest = statisticalTest;
        this.settings = settings;
        this.baselineIndex = baselineIndex;
        this.maxDocs = maxDocs;
    }

    public void run(final Map<String, TermVector> queries,
            final Map<String, Map<String, Double>> rels, final Path resultPath)
            throws IOException {

        // Compute the statistics to be fed to ranker, based on supplied queries
        final Ranker.Statistics statistics = computeStatistics(queries.values());

        final List<QueryEvaluation> evaluations = evaluateQueries(queries, rels, statistics);

        final RankingScore[] scores = aggregateScores(evaluations);

        final Map<Measure, float[]> pvalues = statisticalTest(evaluations);

        // Write aggregate scores
        writeAggregateScores(resultPath, scores, pvalues);

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

        final Cache<Integer, String> docIDs = CacheBuilder.newBuilder().softValues().build();
        final Cache<String, TermVector> docVectors = CacheBuilder.newBuilder().softValues()
                .build();
        for (final String queryID : Ordering.natural().sortedCopy(queries.keySet())) {
            evaluations.add(new QueryEvaluation(queryID, queries.get(queryID), docIDs, docVectors,
                    rels.get(queryID), statistics, this.maxDocs));
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

    private Map<Measure, float[]> statisticalTest(final List<QueryEvaluation> evaluations) {

        // Allocate a map mapping each measure to a vector of p-values (one for each setting)
        final Map<Measure, float[]> pvalues = Maps.newHashMap();

        // Populate the map iterating over considred measures
        final int numQueries = evaluations.size();
        for (final Measure measure : REPORTED_MEASURES) {

            // First, for each setting extract a vector of measure values, one per query
            final double[][] settingsValues = new double[this.settings.length][];
            for (int i = 0; i < this.settings.length; ++i) {
                final double[] values = new double[numQueries];
                for (int j = 0; j < numQueries; ++j) {
                    values[j] = evaluations.get(j).scores[i].get(measure);
                    settingsValues[i] = values;
                }
            }

            // Then, build a vector of p-values, one for each setting for the considered measure
            final float[] pvals = new float[this.settings.length];
            final double[] baselineValues = settingsValues[this.baselineIndex];
            for (int i = 0; i < this.settings.length; ++i) {

                // Count the number of values in baseline/setting vector that are NaN
                int numNaNs = 0;
                for (int j = 0; j < numQueries; ++j) {
                    if (Double.isNaN(settingsValues[i][j]) || Double.isNaN(baselineValues[j])) {
                        ++numNaNs;
                    }
                }

                // Remove NaN values from baseline and setting vectors, if necessary
                double[] baselineVector, settingVector;
                if (numNaNs == 0) {
                    baselineVector = baselineValues;
                    settingVector = settingsValues[i];
                } else {
                    baselineVector = new double[numQueries - numNaNs];
                    settingVector = new double[numQueries - numNaNs];
                    for (int k = 0, j = 0; j < numQueries; ++j) {
                        if (!Double.isNaN(settingsValues[i][j])
                                && !Double.isNaN(baselineValues[j])) {
                            baselineVector[k] = baselineValues[j];
                            settingVector[k] = settingsValues[i][j];
                            ++k;
                        }
                    }
                }

                // Perform the test and store the p-value
                pvals[i] = Float.NaN;
                try {
                    if ("ttest".equalsIgnoreCase(this.statisticalTest)) {
                        pvals[i] = (float) new TTest().pairedTTest(baselineVector, settingVector);
                    } else if ("ar".equalsIgnoreCase(this.statisticalTest)) {
                        pvals[i] = (float) ApproximateRandomization.test(1000, baselineVector,
                                settingVector);
                    }
                } catch (final Throwable ex) {
                    LOGGER.error("Error running statistical test '" + this.statisticalTest + "'",
                            ex);
                }
            }

            // Store the p-value vector in the map
            pvalues.put(measure, pvals);
        }

        // Return the p-value map
        return pvalues;
    }

    private void writeAggregateScores(final Path resultPath, final RankingScore[] scores,
            final Map<Measure, float[]> pvalues) throws IOException {

        final RankingScore[] sortedScores = scores.clone();
        final int[] indexes = sort(sortedScores, RankingScore.comparator(this.sortMeasure, true));

        try (Writer writer = IO.utf8Writer(IO.buffer(
                IO.write(resultPath.resolve("aggregates.csv").toAbsolutePath().toString())))) {
            writer.append("setting");
            for (final Measure measure : REPORTED_MEASURES) {
                writer.append(";").append(measure.toString()).append(";p-value");
            }
            writer.append("\n");
            for (int i = 0; i < sortedScores.length; ++i) {
                writer.append(Joiner.on(",").join(this.settings[indexes[i]]));
                for (final Measure measure : REPORTED_MEASURES) {
                    writer.append(";").append(Double.toString(sortedScores[i].get(measure)));
                    writer.append(";").append(Double.toString(pvalues.get(measure)[indexes[i]]));
                }
                writer.append("\n");
            }
        }
    }

    private void writeSettingScores(final Path resultPath,
            final Iterable<QueryEvaluation> evaluations, final int settingIndex)
            throws IOException {

        try (Writer writer = IO
                .utf8Writer(
                        IO.buffer(
                                IO.write(
                                        resultPath
                                                .resolve("setting-"
                                                        + Joiner.on('-')
                                                                .join(this.settings[settingIndex])
                                                        + ".csv")
                                                .toAbsolutePath().toString())))) {
            writer.append("query");
            for (final Measure measure : REPORTED_MEASURES) {
                writer.append(";").append(measure.toString());
            }
            writer.append(";ranking (top 10);available query layers\n");
            for (final QueryEvaluation evaluation : evaluations) {
                final RankingScore score = evaluation.scores[settingIndex];
                final List<String> queryLayers = Lists.newArrayList(this.settings[settingIndex]);
                queryLayers.retainAll(evaluation.queryVector.getLayers());
                writer.append(evaluation.queryID);
                for (final Measure measure : REPORTED_MEASURES) {
                    writer.append(";").append(Double.toString(score.get(measure)));
                }
                writer.append(';');
                writer.append(Joiner.on(" ")
                        .join(Iterables.limit(Arrays.asList(evaluation.hits[settingIndex]), 10)));
                writer.append(';');
                writer.append(Joiner.on(" ").join(queryLayers));
                writer.append('\n');
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
                .resolve("query-" + evaluation.queryID + ".csv").toAbsolutePath().toString())))) {
            writer.append("setting");
            for (final Measure measure : REPORTED_MEASURES) {
                writer.append(";").append(measure.toString());
            }
            writer.append(";ranking (top 10);available query layers\n");
            for (final RankingScore score : RankingScore.comparator(this.sortMeasure, true)
                    .sortedCopy(Arrays.asList(evaluation.scores))) {
                final int index = map.get(score);
                final List<String> queryLayers = Lists.newArrayList(this.settings[index]);
                queryLayers.retainAll(evaluation.queryVector.getLayers());
                writer.append(Joiner.on(',').join(this.settings[index]));
                for (final Measure measure : REPORTED_MEASURES) {
                    writer.append(";").append(Double.toString(score.get(measure)));
                }
                writer.append(';');
                writer.append(Joiner.on(" ")
                        .join(Iterables.limit(Arrays.asList(evaluation.hits[index]), 10)));
                writer.append(';');
                writer.append(Joiner.on(" ").join(queryLayers));
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
        try (Writer writer = IO.utf8Writer(
                IO.buffer(IO.write(resultPath.resolve("ranking-" + evaluation.queryID + ".csv")
                        .toAbsolutePath().toString())))) {

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
                    builder.append(
                            String.format(" %s=%.3f/", measure.toString(), score.get(measure)));
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
        final Map<T, Integer> map = Maps.newIdentityHashMap();
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

    //    private TermVector toTermVector(final Document document) {
    //        final TermVector.Builder builder = TermVector.builder();
    //        for (final IndexableField field : document.getFields()) {
    //            final String name = field.name();
    //            if (!"id".equals(name)) {
    //                final String value = field.stringValue();
    //                try {
    //                    builder.addTerm(name, value);
    //                } catch (final Throwable ex) {
    //                    // Ignore
    //                }
    //            }
    //        }
    //        final TermVector vector = builder.build();
    //        return vector;
    //    }

    private final class QueryEvaluation implements Runnable {

        // Input

        final String queryID;

        final TermVector queryVector;

        final Cache<Integer, String> cachedDocumentIDs;

        final Cache<String, TermVector> cachedDocumentVectors;

        final Map<String, Double> rels;

        final Ranker.Statistics statistics;

        final Integer maxDocs;

        // Output

        final Hit[][] hits; // a Hit[] for each setting

        final RankingScore[] scores; // a RankingScore for each setting

        QueryEvaluation(final String queryID, final TermVector queryVector,
                final Cache<Integer, String> cachedDocumentIDs,
                final Cache<String, TermVector> cachedDocumentVectors,
                final Map<String, Double> rels, final Ranker.Statistics statistics,
                final Integer maxDocs) {

            this.queryID = queryID;
            this.queryVector = queryVector;
            this.cachedDocumentIDs = cachedDocumentIDs;
            this.cachedDocumentVectors = cachedDocumentVectors;
            this.statistics = statistics;
            this.rels = rels;
            this.hits = new Hit[Evaluation.this.settings.length][];
            this.scores = new RankingScore[Evaluation.this.settings.length];
            this.maxDocs = maxDocs;
        }

        @Override
        public void run() {

            try {
                // Identify matching documents using boolean model (delegated to Lucene, OR semantics)
                final Multimap<String, String> matches = HashMultimap.create();
                final Map<String, TermVector> docVectors = Maps.newHashMap();
                matchDocuments(matches, docVectors);

                // Compute and evaluate rankings for each setting
                rankDocuments(matches, docVectors);

                // Log number of hits and best ranking, if enabled
                logCompletion();

            } catch (final Throwable ex) {
                // Wrap and propagate
                throw Throwables.propagate(ex);
            }
        }

        private void matchDocuments(final Multimap<String, String> matches,
                final Map<String, TermVector> vectors) throws IOException, ParseException {

            // Evaluate a Lucene query for each layer and populate a multimap with matched document IDs
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
                final TopDocs results = Evaluation.this.searcher.search(query, this.maxDocs);
                LOGGER.debug("{} results obtained from query {}", results.scoreDocs.length,
                        queryString);

                // Populate the matches multimap. This requires mapping the numerical doc ID to
                // the corresponding String one. We also retrieve the associated Lucene document
                // and cache the document term vector for later reuse.
                for (final ScoreDoc scoreDoc : results.scoreDocs) {
                    String docID = this.cachedDocumentIDs.getIfPresent(scoreDoc.doc);
                    TermVector docVector = docID == null ? null
                            : this.cachedDocumentVectors.getIfPresent(docID);
                    if (docVector == null) {
                        final Document doc = Evaluation.this.searcher.doc(scoreDoc.doc);
                        docID = doc.get("id").intern();
                        docVector = TermVector.read(doc);
                        this.cachedDocumentIDs.put(scoreDoc.doc, docID);
                        this.cachedDocumentVectors.put(docID, docVector);
                    }
                    matches.put(layer, docID);
                    vectors.put(docID, docVector);
                }
            }
        }

        private void rankDocuments(final Multimap<String, String> matches,
                final Map<String, TermVector> docVectors) {

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
                        vectors[j] = docVectors.get(ids[j]);
                    }
                    final float[] scores = Evaluation.this.ranker.rank(
                            this.queryVector.project(Arrays.asList(setting)), vectors,
                            this.statistics);

                    // Build and store a sorted list of Hit objects, removing documents scored 0
                    final List<Hit> hitList = Lists.newArrayList();
                    for (int j = 0; j < ids.length; ++j) {
                        if (scores[j] > 0.0f) {
                            hitList.add(new Hit(ids[j], scores[j]));
                        }
                    }
                    final Hit[] hits = hitList.toArray(new Hit[hitList.size()]);
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
                    if (bestIndex < 0 || RankingScore.comparator(Evaluation.this.sortMeasure, true)
                            .compare(this.scores[i], this.scores[bestIndex]) <= 0) {
                        bestIndex = i;
                    }
                }
                LOGGER.info("Evaluated {} - {} hits, best: {} {}", this.queryID,
                        String.format("%4d", numHits), this.scores[bestIndex],
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
            builder.append('/');
            builder.append(String.format("%.3f", this.score));
            return builder.toString();
        }

    }

}
