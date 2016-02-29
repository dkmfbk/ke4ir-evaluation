package eu.fbk.pikesir;

import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;

public abstract class Ranker {

    public abstract float[] rank(TermVector queryVector, TermVector[] docVectors, Stats stats);

    public static final Ranker createTfIdfRanker(final float semanticWeight) {
        return new TfIdfRanker(semanticWeight);
    }

    public static final class Stats {

        private final Map<String, CollectionStatistics> layerStats;

        private final Map<Term, TermStatistics> termStats;

        Stats(final Map<String, CollectionStatistics> layerStats,
                final Map<Term, TermStatistics> termStats) {
            this.layerStats = ImmutableMap.copyOf(layerStats);
            this.termStats = ImmutableMap.copyOf(termStats);
        }

        public Set<String> getLayers() {
            return this.layerStats.keySet();
        }

        public long getNumDocuments() {
            final String layer = this.layerStats.keySet().iterator().next();
            return this.layerStats.get(layer).maxDoc();
        }

        public long getNumDocuments(final String layer) {
            return this.layerStats.get(layer).docCount();
        }

        public long getNumDocuments(final Term term) {
            return this.termStats.get(term).docFreq();
        }

        public long getNumTermsTotal(final String layer) {
            return this.layerStats.get(layer).sumTotalTermFreq();
        }

        public long getTotalFrequency(final Term term) {
            return this.termStats.get(term).totalTermFreq();
        }

    }

    private static final class TfIdfRanker extends Ranker {

        private final float semanticWeight;

        public TfIdfRanker(final float semanticWeight) {
            Preconditions.checkArgument(semanticWeight >= 0.0f && semanticWeight <= 1.0);
            this.semanticWeight = semanticWeight;
        }

        @Override
        public float[] rank(final TermVector queryVector, final TermVector[] docVectors,
                final Stats stats) {

            final Set<String> queryLayers = Sets.newHashSet();
            for (final Term term : queryVector.getTerms()) {
                if (!"textual".equals(term.getField())) {
                    queryLayers.add(term.getField());
                }
            }

            final long numDocs = stats.getNumDocuments();

            final float[] scores = new float[docVectors.length];

            for (final Term queryTerm : queryVector.getTerms()) {

                final long docFreq = stats.getNumDocuments(queryTerm);

                final String layer = queryTerm.getField();
                final float weight = "textual".equals(layer) ? 1.0f - this.semanticWeight
                        : this.semanticWeight / queryLayers.size();

                for (int i = 0; i < docVectors.length; ++i) {

                    final Term docTerm = docVectors[i].getTerm(layer, queryTerm.getValue());
                    if (docTerm == null) {
                        continue;
                    }

                    // final float tfd = freq <= 0.0f ? 0.0f : (float) (1.0 + Math.log(freq)); // paper
                    final float tfd = (float) Math.sqrt(docTerm.getWeight()); // test and lucene

                    final float tfq = (float) queryTerm.getWeight();

                    // final float idf = (float) Math.log(numDocs / (double) docFreq); // paper
                    final float idf = (float) (Math.log(numDocs / (double) (docFreq + 1)) + 1.0); // test

                    scores[i] += tfd * idf * idf * tfq * weight;
                }
            }

            return scores;
        }

    }

}
