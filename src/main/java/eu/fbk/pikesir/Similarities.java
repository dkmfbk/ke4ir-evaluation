package eu.fbk.pikesir;

import java.nio.file.Path;
import java.util.Properties;

import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.BasicStats;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.SimilarityBase;

public class Similarities {

    public static Similarity create(final Path root, final Properties properties, String prefix) {

        // Normalize prefix, ensuring it ends with '.'
        prefix = prefix.endsWith(".") ? prefix : prefix + ".";

        // Create a TfIdf similarity, if enabled
        if (Boolean.parseBoolean(properties.getProperty(prefix + "tfidf", "false"))) {
            return new SimilarityAdapter();
            // return createTfIdfSimilarity();
        }

        // Otherwise return default similarity
        return new ClassicSimilarity();
    }

    private static class SimilarityAdapter extends SimilarityBase {

        @Override
        protected BasicStats newStats(final String field) {
            return new Stats(field);
        }

        @Override
        protected void fillBasicStats(final BasicStats stats,
                final CollectionStatistics collectionStats, final TermStatistics termStats) {
            super.fillBasicStats(stats, collectionStats, termStats);
            ((Stats) stats).value = termStats.term().utf8ToString();
        }

        @Override
        protected float score(final BasicStats basicStats, final float freq, final float docLen) {

            final Stats stats = (Stats) basicStats;
            // System.out.println(stats.value);

            // final Term docTerm = Term.create(Field.forID(stats.field), stats.value);

            final long numDocs = stats.getNumberOfDocuments();
            final long docFreq = stats.getDocFreq();

            // final float tfd = freq <= 0.0f ? 0.0f : (float) (1.0 + Math.log(freq)); // paper
            final float tfd = (float) Math.sqrt(freq); // test and lucene

            final float tfq = stats.getBoost();

            // final float idf = (float) Math.log(numDocs / (double) docFreq); // paper
            final float idf = (float) (Math.log(numDocs / (double) (docFreq + 1)) + 1.0); // test

            return tfd * idf * idf * tfq;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName();
        }

        private static class Stats extends BasicStats {

            public Stats(final String field) {
                super(field);
                this.field = field;
            }

            String value;

            String field;

        }

    }

}
