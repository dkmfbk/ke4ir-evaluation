package eu.fbk.pikesir;

import java.nio.file.Path;
import java.util.Properties;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.Similarity;

public class Similarities {

    public static Similarity createTfIdfSimilarity(final boolean enableCoord,
            final boolean enableNorm, final boolean enableQNorm) {
        return new TfIdfSimilarity(enableCoord, enableNorm, enableQNorm);
    }

    public static Similarity create(final Path root, final Properties properties, String prefix) {

        // Normalize prefix, ensuring it ends with '.'
        prefix = prefix.endsWith(".") ? prefix : prefix + ".";

        // Create a TfIdf similarity, if enabled
        if (Boolean.parseBoolean(properties.getProperty(prefix + "tfidf", "false"))) {
            final boolean enableCoord = Boolean.parseBoolean( //
                    properties.getProperty(prefix + "tfidf.coord", "false"));
            final boolean enableNorm = Boolean.parseBoolean( //
                    properties.getProperty(prefix + "tfidf.norm", "false"));
            final boolean enableQNorm = Boolean.parseBoolean( //
                    properties.getProperty(prefix + "tfidf.qnorm", "false"));
            return createTfIdfSimilarity(enableCoord, enableNorm, enableQNorm);
        }

        // Otherwise return default similarity
        return new ClassicSimilarity();
    }

    private static class TfIdfSimilarity extends ClassicSimilarity {

        private final boolean enableCoord;

        private final boolean enableNorm;

        private final boolean enableQNorm;

        TfIdfSimilarity(final boolean enableCoord, final boolean enableNorm,
                final boolean enableQNorm) {
            this.enableCoord = enableCoord;
            this.enableNorm = enableNorm;
            this.enableQNorm = enableQNorm;
        }

        @Override
        public float idf(final long docFreq, final long numDocs) {
            return (float) (Math.log(numDocs / (double) (docFreq + 1)) + 1.0); // used in tests
            // return (float) Math.log(numDocs / (double) docFreq); // used in paper
        }

        // uncomment to print idf values
        //        @Override
        //        public Explanation idfExplain(final CollectionStatistics collectionStats,
        //                final TermStatistics termStats) {
        //            final Explanation e = super.idfExplain(collectionStats, termStats);
        //            System.err.println(collectionStats.field() + " - " + termStats.term().utf8ToString()
        //                    + " - " + e);
        //            return e;
        //        }

        @Override
        public float tf(final float freq) {
            return (float) Math.sqrt(freq); // used in tests and by Lucene
            // return freq; // standard formula referenced by reviewer
            // return freq <= 0.0f ? 0.0f : (float) (1.0 + Math.log(freq)); // used in paper
        }

        @Override
        public float queryNorm(final float sumOfSquaredWeights) {
            return this.enableQNorm ? super.queryNorm(sumOfSquaredWeights) : 1.0f;
        }

        @Override
        public float coord(final int overlap, final int maxOverlap) {
            return this.enableCoord ? super.coord(overlap, maxOverlap) : 1.0f;
        }

        @Override
        public float lengthNorm(final FieldInvertState state) {
            return this.enableNorm ? super.lengthNorm(state) : 1.0f;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(coord: " + this.enableCoord + ", norm: "
                    + this.enableNorm + ")";
        }

    }

}
