package eu.fbk.pikesir;

import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;

/**
 * Ranks the documents returned by a query, implementing some ranking/similarity method.
 */
public abstract class Ranker {

    /**
     * Ranks the document returned by a query (under the boolean model). The method receives the
     * term vector of the query, the term vectors of all the documents matching the query and a
     * {@link Statistics} object containing some index-wide statistics (e.g., number of documents
     * overall and containing a certain term). The method must return an array of scores, with
     * {@code score[i]} associated to document {@code docVector[i]}, which is used by the system
     * to rank the documents (from highest to lowest score - in case of equal scores,
     * corresponding documents are ranked by increasing document ID).
     *
     * @param queryVector
     *            the term vector associated to the query,
     * @param docVectors
     *            the term vectors associated to the documents matched by the query
     * @param statistics
     *            additional statistics that can be queried by the {@code Ranker}
     * @return a score array, where i-th element corresponds to i-th document
     */
    public abstract float[] rank(TermVector queryVector, TermVector[] docVectors,
            Statistics statistics);

    /**
     * {@inheritDoc} Emits a descriptive string describe the Ranker and its configuration. This
     * implementation emits the class name.
     */
    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    /**
     * Creates a {@code Ranker} that ranks documents w.r.t. a query based on a TF/IDF criterion. A
     * weight is applied to each term to take into account its layer. The total weight of semantic
     * layers is fixed and distributed evenly among them based on the actual layers available in
     * the query. Parameter {@code textualLayer} allows to discriminate between textual and
     * non-textual layers.
     *
     * @param textualLayer
     *            the name of the textual layer
     * @param semanticWeight
     *            the total weight associated to semantic layers
     * @return the created Ranker
     */
    public static Ranker createTfIdfRanker(final String textualLayer, final float semanticWeight) {
        return new TfIdfRanker(textualLayer, semanticWeight);
    }

    /**
     * Returns a {@code Ranker} based on the configuration properties supplied. The method looks
     * for certain properties within the {@link Properties} object supplied, prepending them an
     * optional prefix (e.g., if property is X and the prefix is Y, the method will look for
     * property Y.X). The path parameter is used as the base directory for resolving relative
     * paths contained in the examined properties. The properties currently supported are:
     * <ul>
     * <li>{@code type} - the type of {@code Ranker} to instantiate; currently only {@code tfidf}
     * is supported (corresponding to {@link #createTfIdfRanker(String, float)});</li>
     * <li>{@code tfidf.semanticweight} - the total weight assigned to semantic layers in the
     * TF/IDF {@code Ranker};</li>
     * <li>{@code tfidf.textuallayer} - the name of the textual layer, used by the TF/IDF
     * {@code Ranker}.</li>
     * </ul>
     *
     * @param root
     *            the base directory for resolving relative paths
     * @param properties
     *            the configuration properties
     * @param prefix
     *            an optional prefix to prepend to supported properties
     * @return a {@code Ranker} based on the specified configuration (if successful)
     */
    public static Ranker create(final Path root, final Properties properties, String prefix) {

        // Normalize prefix, ensuring it ends with '.'
        prefix = prefix.endsWith(".") ? prefix : prefix + ".";

        // Choose the type of Ranker based on the 'type' property
        final String type = properties.getProperty(prefix + "type", "");
        switch (type) {
        case "tfidf":
            final String textualLayer = properties.getProperty(prefix + "tfidf.textuallayer",
                    "textual");
            final float semanticWeight = Float.parseFloat(properties.getProperty(prefix
                    + "tfidf.semanticweight", "0.5"));
            return createTfIdfRanker(textualLayer, semanticWeight);

        default:
            throw new IllegalArgumentException("Unsupported Ranker type: " + type);
        }
    }

    private static final class TfIdfRanker extends Ranker {

        private final String textualLayer;

        private final float semanticWeight;

        public TfIdfRanker(final String textualLayer, final float semanticWeight) {
            Preconditions.checkArgument(semanticWeight >= 0.0f && semanticWeight <= 1.0);
            this.textualLayer = Objects.requireNonNull(textualLayer);
            this.semanticWeight = semanticWeight;
        }

        @Override
        public float[] rank(final TermVector queryVector, final TermVector[] docVectors,
                final Statistics stats) {

            // Retrieve the set of semantic layers available in the query
            final Set<String> queryLayers = Sets.newHashSet();
            for (final Term term : queryVector.getTerms()) {
                if (!this.textualLayer.equals(term.getField())) {
                    queryLayers.add(term.getField());
                }
            }

            final long numDocs = stats.getNumDocuments();

            final float[] scores = new float[docVectors.length];

            // We compute the score of each document by iterating first on query terms (so that
            // some state can be saved and reused) and then on document vectors. The relative
            // order of the two iterations is irrelevant.
            for (final Term queryTerm : queryVector.getTerms()) {

                // Extract term layer and associated weight. Note that for semantic layers we
                // divide the total semantic weight among the layers effectively available in the
                // query, and NOT all the semantic layers used in the index
                final String layer = queryTerm.getField();
                final float weight = this.textualLayer.equals(layer) ? 1.0f - this.semanticWeight
                        : this.semanticWeight / queryLayers.size();

                // Extract the document frequency (# documents having that term in the index)
                final long docFreq = stats.getNumDocuments(queryTerm);

                // Iterate over documents, increasing their score based on how they match the
                // query term being currently considered
                for (int i = 0; i < docVectors.length; ++i) {

                    // Abort in case the document does not contain the query term
                    final Term docTerm = docVectors[i].getTerm(layer, queryTerm.getValue());
                    if (docTerm == null) {
                        continue;
                    }

                    // TODO

                    // Compute TF document side (tfd), TF query side (tfq) and IDF

                    //                    final float tfq;
                    //                    if (layer.equals(this.textualLayer)) {
                    //                        tfq = (float) (1.0 + Math.log(queryTerm.getWeight()));
                    //                    } else {
                    //                        tfq = (float) queryTerm.getWeight();
                    //                    }

                    final double freq = docTerm.getFrequency();
                    final float tfd = freq <= 0.0f ? 0.0f : (float) (1.0 + Math.log(freq)); // paper
                    final float tfq = (float) queryTerm.getWeight();
                    final float idf = (float) Math.log(numDocs / (double) docFreq); // paper

                    //                    final float tfd = (float) Math.sqrt(freq); // test and lucene
                    //                    final float idf = (float) (Math.log(numDocs / (double) (docFreq + 1)) + 1.0); // test

                    // Update the document score
                    scores[i] += tfd * idf * idf * tfq * weight;
                }
            }

            // Return the computed scores
            return scores;
        }

    }

    /**
     * Index statistics exposed to {@code Ranker} objects.
     */
    public static final class Statistics {

        private final Map<String, CollectionStatistics> layerStats;

        private final Map<Term, TermStatistics> termStats;

        Statistics(final Map<String, CollectionStatistics> layerStats,
                final Map<Term, TermStatistics> termStats) {
            this.layerStats = ImmutableMap.copyOf(layerStats);
            this.termStats = ImmutableMap.copyOf(termStats);
        }

        /**
         * Returns the layers configured in the index.
         *
         * @return the set of layers in the system
         */
        public Set<String> getLayers() {
            return this.layerStats.keySet();
        }

        /**
         * Returns the total number of documents in the index.
         *
         * @return the total number of documents
         */
        public long getNumDocuments() {
            final String layer = this.layerStats.keySet().iterator().next();
            return this.layerStats.get(layer).maxDoc();
        }

        /**
         * Returns the number of documents containing some term of the layer specified.
         *
         * @param layer
         *            the layer name
         * @return the number of documents having terms of the layer specified
         */
        public long getNumDocuments(final String layer) {
            return this.layerStats.get(layer).docCount();
        }

        /**
         * Returns the number of documents containing the term specified (i.e., the document
         * frequency used for IDF computation).
         *
         * @param term
         *            the term
         * @return the number of documents with that term
         */
        public long getNumDocuments(final Term term) {
            return this.termStats.get(term).docFreq();
        }

        /**
         * Returns the total number of terms in the layer specified
         *
         * @param layer
         *            the layer name
         * @return the number of terms in the layer
         */
        public long getNumTermsTotal(final String layer) {
            return this.layerStats.get(layer).sumTotalTermFreq();
        }

        /**
         * Returns the cumulated frequency in the whole index of the term specified
         *
         * @param term
         *            the term
         * @return the cumulated raw frequency of the term
         */
        public long getTotalFrequency(final Term term) {
            return this.termStats.get(term).totalTermFreq();
        }

    }

}
