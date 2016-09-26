package eu.fbk.ke4ir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.lucene.index.TermContext;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.IndexSearcher;
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
            Statistics statistics, Boolean normalize);

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
     * weight is applied to each term to take into account its layer, according to the supplied
     * layer-to-weight map. The optional parameter {@code rescaledLayers} list the layers for
     * which the sum of weights must be guaranteed to be constant. If non-empty and the query does
     * not have one of the layers in {@code rescaledLayers}, then the weight of the other layer in
     * {@code rescaledLayers} is rescaled linearly so to guarantee that the sum of their new
     * weights produces the expected sum. This mechanism can be used, e.g., to guarantee that the
     * total weight assigned to <it>available</it> semantic layers is a certain constant (e.g.,
     * 0.5).
     *
     * @param sectionWeights
     *            a section-to-weight map; if a section weight is missing in this map, it is
     *            assumed to be 0
     * @param layerWeights
     *            a layer-to-weight map; if a layer weight is missing in this map, it is assumed
     *            to be 0
     * @param rescaledLayers
     *            the layers for which to apply the rescaling mechanism; leave empty in order not
     *            to rescale anything
     * @return the created Ranker
     */
    public static Ranker createTfIdfRanker(final Map<String, Float> sectionWeights,
            final Map<String, Float> layerWeights,
            @Nullable final Iterable<String> rescaledLayers) {
        return new TfIdfRanker(sectionWeights, layerWeights, rescaledLayers);
    }

    /**
     * Returns a {@code Ranker} based on the configuration properties supplied. The method looks
     * for certain properties within the {@link Properties} object supplied, prepending them an
     * optional prefix (e.g., if property is X and the prefix is Y, the method will look for
     * property Y.X). The path parameter is used as the base directory for resolving relative
     * paths contained in the examined properties. The properties currently supported are:
     * <ul>
     * <li>{@code type} - the type of {@code Ranker} to instantiate; currently only {@code tfidf}
     * is supported (corresponding to {@link createTfIdfRanker(String, float)});</li>
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
            final Map<String, Float> sectionWeights = parseMap(
                    properties.getProperty(prefix + "tfidf.sectionweights", ""));
            final Map<String, Float> layerWeights = parseMap(
                    properties.getProperty(prefix + "tfidf.weights", ""));
            final Set<String> rescaledLayers = ImmutableSet.copyOf(
                    properties.getProperty(prefix + "tfidf.rescaling", "").trim().split("\\s+"));
            if (sectionWeights.isEmpty()) {
                sectionWeights.put("", 1.0f);
            }
            return createTfIdfRanker(sectionWeights, layerWeights, rescaledLayers);

        default:
            throw new IllegalArgumentException("Unsupported Ranker type: " + type);
        }
    }

    private static Map<String, Float> parseMap(@Nullable final String string) {
        final Map<String, Float> map = Maps.newHashMap();
        if (string != null) {
            for (final String pair : string.trim().split("\\s+")) {
                if (!pair.isEmpty()) {
                    final int index = pair.indexOf(':');
                    final String layer = pair.substring(0, index).trim();
                    final float weight = Float.parseFloat(pair.substring(index + 1));
                    map.put(layer, weight);
                }
            }
        }
        return map;
    }

    private static final class TfIdfRanker extends Ranker {

        private final Map<String, Float> sectionWeights;

        private final Map<String, Float> layerWeights;

        private final Set<String> rescaledLayers;

        public TfIdfRanker(final Map<String, Float> sectionWeights,
                final Map<String, Float> layerWeights,
                @Nullable final Iterable<String> rescaledLayers) {
            this.sectionWeights = ImmutableMap.copyOf(sectionWeights);
            this.layerWeights = ImmutableMap.copyOf(layerWeights);
            this.rescaledLayers = rescaledLayers == null ? ImmutableSet.of()
                    : ImmutableSet.copyOf(rescaledLayers);
        }

        @Override
        public float[] rank(final TermVector queryVector, final TermVector[] docVectors,
                final Statistics stats, final Boolean normalize) {

            // Apply weight rescaling, if configured to do so
            Map<String, Float> layerWeights = this.layerWeights;
            if (!this.rescaledLayers.isEmpty()) {
                float sumBefore = 0.0f;
                float sumAfter = 0.0f;
                for (final String layer : this.rescaledLayers) {
                    final float weight = layerWeights.getOrDefault(layer, 0.0f);
                    sumBefore += queryVector.getLayers().contains(layer) ? weight : 0.0f;
                    sumAfter += weight;
                }
                if (sumBefore > 0.0f && sumBefore != sumAfter) {
                    final float multiplier = sumAfter / sumBefore;
                    layerWeights = Maps.newHashMap(layerWeights);
                    for (final String layer : this.rescaledLayers) {
                        layerWeights.put(layer,
                                layerWeights.getOrDefault(layer, 0.0f) * multiplier);
                    }
                }
            }

            // Retrieve the number of documents from Lucene statistics
            final long numDocs = stats.getNumDocuments();

            // Allocate the array of document scores, one element for each document vector
            final float[] scores = new float[docVectors.length];

            for (final Entry<String, Float> entry : this.sectionWeights.entrySet()) {

                final String section = entry.getKey();
                final float sectionWeight = entry.getValue();

                final float[] norm = new float[docVectors.length];
                if (normalize) {
                    final Set<String> sectionLayers = Sets.newHashSet();
                    for (final String layer : this.layerWeights.keySet()) {
                        sectionLayers.add(section + layer);
                    }
                    for (int i = 0; i < docVectors.length; ++i) {
                        norm[i] = 0.0f;
                        for (final Term docTerm : docVectors[i].getTerms()) {
                            if (sectionLayers.contains(docTerm.getField())) {
                                final double rfd = docTerm.getFrequency(); // raw frequency, document side
                                final float tfd = (float) (1.0 + Math.log(rfd)); // TF, document side
                                final float idf = (float) Math
                                        .log(numDocs / (double) stats.getNumDocuments(docTerm)); // IDF
                                norm[i] += tfd * tfd * idf * idf;
                            }
                        }
                        norm[i] = (float) Math.sqrt(norm[i]);
                    }
                }

                // We compute the score of each document by iterating first on query terms (so that
                // some state can be saved and reused) and then on document vectors. The relative
                // order of the two iterations is irrelevant.
                for (final Term queryTerm : queryVector.getTerms()) {

                    // Extract term layer and associated weight.
                    final String layer = queryTerm.getField();
                    final String sectionLayer = section + layer;
                    final float layerWeight = layerWeights.getOrDefault(layer, 0.0f);

                    // Extract the document frequency (# documents having that term in the index)
                    final long docFreq = stats
                            .getNumDocuments(Term.create(sectionLayer, queryTerm.getValue()));

                    // Iterate over documents, increasing their score based on how they match the
                    // query term being currently considered
                    for (int i = 0; i < docVectors.length; ++i) {

                        // Skip in case the document does not contain the query term
                        final Term docTerm = docVectors[i].getTerm(sectionLayer,
                                queryTerm.getValue());
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
                        if (normalize) {
                            scores[i] += tfd * idf * idf * tfq * layerWeight * sectionWeight
                                    / norm[i];
                        } else {
                            scores[i] += tfd * idf * idf * tfq * layerWeight * sectionWeight;
                        }
                    }

                }
            }

            // Return the computed scores
            return scores;
        }

        @Override
        public String toString() {
            return super.toString() + "(sectionWeights: " + this.sectionWeights
                    + ", layerWeights: " + this.layerWeights + ", rescaledLayers: "
                    + this.rescaledLayers + ")";
        }
    }

    /**
     * Index statistics exposed to {@code Ranker} objects.
     */
    public static final class Statistics {

        private final Set<String> sections;

        private final Set<String> layers;

        private final Map<String, CollectionStatistics> layerStats;

        private final Map<Term, TermStatistics> termStats;

        private final IndexSearcher searcher;

        Statistics(final Iterable<String> sections, final Iterable<String> layers,
                final Map<String, CollectionStatistics> layerStats,
                final Map<Term, TermStatistics> termStats, final IndexSearcher searcher) {
            this.sections = ImmutableSet.copyOf(sections);
            this.layers = ImmutableSet.copyOf(layers);
            this.layerStats = ImmutableMap.copyOf(layerStats);
            //this.termStats = ImmutableMap.copyOf(termStats);
            this.termStats = termStats;
            this.searcher = searcher;
        }

        /**
         * Returns the sections (section prefixes) configured in the index.
         *
         * @return the set of sections
         */
        public Set<String> getSections() {
            return this.sections;
        }

        /**
         * Returns the layers configured in the index.
         *
         * @return the set of layers in the system
         */
        public Set<String> getLayers() {
            return this.layers;
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
         * Returns the number of documents containing some term of the section/layer specified.
         *
         * @param sectionLayer
         *            the combination section + layer
         * @return the number of documents having terms of the layer specified
         */
        public long getNumDocuments(final String sectionLayer) {
            return this.layerStats.get(sectionLayer).docCount();
        }

        /**
         * Returns the number of documents containing the document term specified (i.e., the
         * document frequency used for IDF computation).
         *
         * @param term
         *            the term (beware that the field should be section + layer)
         * @return the number of documents with that term
         */
        public synchronized long getNumDocuments(final Term term) {

            TermStatistics t = this.termStats.get(term);
            if (t == null) {

                final String sectionLayer = term.getField();
                final String value = term.getValue();
                final org.apache.lucene.index.Term luceneTerm;
                luceneTerm = new org.apache.lucene.index.Term(sectionLayer, value);

                try {
                    t = this.searcher.termStatistics(luceneTerm, //
                            TermContext.build(this.searcher.getTopReaderContext(), luceneTerm));
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                }

                this.termStats.put(term, t);

            }
            return t.docFreq();

        }

        /**
         * Returns the total number of terms in the section/layer specified
         *
         * @param sectionLayer
         *            the section + layer pair
         * @return the number of terms in the layer
         */
        public long getNumTermsTotal(final String sectionLayer) {
            return this.layerStats.get(sectionLayer).sumTotalTermFreq();
        }

        /**
         * Returns the cumulated frequency in the whole index of the term specified
         *
         * @param term
         *            the term (beware that the field should be section + layer)
         * @return the cumulated raw frequency of the term
         */
        public long getTotalFrequency(final Term term) {
            return this.termStats.get(term).totalTermFreq();
        }

    }

}
