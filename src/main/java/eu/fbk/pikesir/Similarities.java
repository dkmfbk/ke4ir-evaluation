package eu.fbk.pikesir;

import java.nio.file.Path;
import java.util.Properties;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.Similarity;

public class Similarities {

    public static Similarity createTfIdfSimilarity() {
        return new TfIdfSimilarity();
    }

    public static Similarity create(final Path root, final Properties properties, String prefix) {

        // Normalize prefix, ensuring it ends with '.'
        prefix = prefix.endsWith(".") ? prefix : prefix + ".";

        // Create a TfIdf similarity, if enabled
        if (Boolean.parseBoolean(properties.getProperty(prefix + "tfidf", "false"))) {
            return createTfIdfSimilarity();
        }

        // Otherwise return default similarity
        return new ClassicSimilarity();
    }

    private static class TfIdfSimilarity extends ClassicSimilarity {

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
            return 1.0f;
        }

        @Override
        public float coord(final int overlap, final int maxOverlap) {
            return 1.0f;
        }

        @Override
        public float lengthNorm(final FieldInvertState state) {
            return 1.0f;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName();
        }

    }

}
