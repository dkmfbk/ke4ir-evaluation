package eu.fbk.pikesir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;

import eu.fbk.pikesir.util.Util;

public abstract class Aggregator {

    public abstract void aggregate(final List<String> allLayers, final List<String> queryLayers,
            final TermVector queryVector, final List<Hit> hits, final List<TermVector> docVectors);

    public static Aggregator createBalanced(final boolean normalize, final String textLayer,
            final double textWeight, final Map<String, Double> semanticWeights) {
        return new BalancedAggregator(normalize, textLayer, textWeight, semanticWeights);
    }

    public static Aggregator create(final Path root, final Properties properties, String prefix) {

        // Normalize prefix, ensuring it ends with '.'
        prefix = prefix.endsWith(".") ? prefix : prefix + ".";

        // Select the type of aggregator based on property 'type'
        final String type = properties.getProperty(prefix + "type", "sum").trim().toLowerCase();
        if (type.equals("balanced")) {
            final String textLayer = properties.getProperty(prefix + "balanced.textlayer",
                    "textual");
            final double textWeight = Double.parseDouble(properties.getProperty(prefix
                    + "balanced.textweight", "0.5"));
            final Map<String, Double> semanticWeights = Util.parseMap(
                    properties.getProperty(prefix + "balanced.semanticweights"), Double.class);
            return createBalanced(false, textLayer, textWeight, semanticWeights);
        } else {
            throw new IllegalArgumentException("Invalid aggregator type '" + type + "'");
        }
    }

    private static abstract class SimpleAggregator extends Aggregator {

        final boolean normalize;

        SimpleAggregator(final boolean normalize) {
            this.normalize = normalize;
        }

        @Override
        public void aggregate(final List<String> allLayers, final List<String> queryLayers,
                final TermVector queryVector, final List<Hit> hits,
                final List<TermVector> docVectors) {

            // We operate only on query layers, i.e., the layers present in the query
            final String[] layers = queryLayers.toArray(new String[queryLayers.size()]);
            final int numLayers = layers.length;

            // If normalization is enabled, obtain max scores for each layer
            double[] normMultipliers = null;
            if (this.normalize) {
                final double[] minScores = new double[numLayers];
                final double[] maxScores = new double[numLayers];
                final double[] sumScores = new double[numLayers];
                for (final Hit hit : hits) {
                    for (int i = 0; i < numLayers; ++i) {
                        final double score = hit.getLayerScore(layers[i]);
                        minScores[i] = minScores[i] == 0.0 ? score : Math.min(minScores[i], score);
                        maxScores[i] = Math.max(maxScores[i], score);
                        sumScores[i] += score;
                    }
                }
                normMultipliers = new double[numLayers];
                for (int i = 0; i < numLayers; ++i) {
                    normMultipliers[i] = maxScores[i] == 0.0 ? 0.0 : 1.0 / maxScores[i];
                }
            }

            // Assign aggregate scores, delegating to aggregate(double[])
            final double[] scores = new double[numLayers];
            for (int i = 0; i < hits.size(); ++i) {
                final Hit hit = hits.get(i);
                final TermVector docVector = docVectors.get(i);
                for (int j = 0; j < numLayers; ++j) {
                    double score = hit.getLayerScore(layers[j]);
                    if (this.normalize) {
                        score = score * normMultipliers[j];
                    }
                    scores[j] = score;
                }
                hit.setAggregateScore(aggregate(queryLayers, queryVector, docVector, scores));
            }
        }

        abstract double aggregate(List<String> layers, TermVector queryVector,
                TermVector docVector, double[] scores);

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(normalize: " + this.normalize + ")";
        }

    }

    private static final class BalancedAggregator extends SimpleAggregator {

        private final String textLayer;

        private final double textWeight;

        private final Map<String, Double> semanticWeights;

        BalancedAggregator(final boolean normalize, final String textLayer,
                final double textWeight, final Map<String, Double> semanticWeights) {
            super(normalize);
            this.textLayer = Objects.requireNonNull(textLayer);
            this.textWeight = textWeight;
            this.semanticWeights = ImmutableMap.copyOf(semanticWeights);
        }

        @Override
        double aggregate(final List<String> layers, final TermVector queryVector,
                final TermVector docVector, final double[] scores) {

            double textScore = 0.0;

            double semanticSum = 0.0;
            double semanticWeight = 0.0;

            for (int i = 0; i < scores.length; ++i) {
                final String layer = layers.get(i);
                final double score = scores[i];
                if (layer.equals(this.textLayer)) {
                    textScore = score;
                } else {
                    final double weight = MoreObjects.firstNonNull(
                            this.semanticWeights.get(layer), 0.0);
                    semanticSum += weight * score;
                    semanticWeight += weight;
                }
            }

            // FIXME

            final double semanticScore = semanticWeight == 0.0 ? 0.0 : semanticSum
                    / semanticWeight;

            return this.textWeight * textScore + (1.0 - this.textWeight) * semanticScore;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(text: " + this.textLayer + ", text weight: "
                    + this.textWeight + ", semantic weights: " + this.semanticWeights + ")";
        }

    }

}
