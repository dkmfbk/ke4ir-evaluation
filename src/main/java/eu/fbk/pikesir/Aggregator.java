package eu.fbk.pikesir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.primitives.Doubles;

public abstract class Aggregator {

    public abstract void aggregate(final List<String> allLayers, final List<String> queryLayers,
            final List<Hit> hits);

    public static Aggregator createWeightedAggregator(final boolean normalize,
            final Map<String, Double> layerWeights) {
        return new WeightedAggregator(normalize, layerWeights);
    }

    public static Aggregator createSumAggregator(final boolean normalize) {
        return new SumAggregator(normalize);
    }

    public static Aggregator createProductAggregator(final boolean normalize) {
        return new ProductAggregator(normalize);
    }

    public static Aggregator createPrioritizedAggregator(final boolean normalize) {
        return new PrioritizedAggregator(normalize);
    }

    public static Aggregator createMeanAggregator(final boolean normalize) {
        return new MeanAggregator(normalize);
    }

    public static Aggregator createMaxAggregator(final boolean normalize) {
        return new MaxAggregator(normalize);
    }

    public static Aggregator createMinAggregator(final boolean normalize) {
        return new MinAggregator(normalize);
    }

    public static Aggregator create(final Path root, final Properties properties, String prefix) {

        // Normalize prefix, ensuring it ends with '.'
        prefix = prefix.endsWith(".") ? prefix : prefix + ".";

        // Determine whether normalization is enabled or not
        final boolean normalize = Boolean.parseBoolean(properties.getProperty(
                prefix + "normalize", "false"));

        // Select the type of aggregator based on property 'type'
        final String type = properties.getProperty(prefix + "type", "sum").trim().toLowerCase();
        if (type.equals("weighted")) {
            final Map<String, Double> layerWeights = Maps.newHashMap();
            final String weightsSpec = properties.getProperty(prefix + "weights");
            if (weightsSpec != null) {
                for (final String weightSpec : weightsSpec.split("[\\s;]+")) {
                    final int index = Math.max(weightSpec.indexOf(':'), weightSpec.indexOf('='));
                    if (index > 0) {
                        final String layer = weightSpec.substring(0, index).trim();
                        final Double weight = Double.parseDouble(weightSpec.substring(index + 1));
                        layerWeights.put(layer, weight);
                    }
                }
            }
            return createWeightedAggregator(normalize, layerWeights);
        } else if (type.equals("sum")) {
            return createSumAggregator(normalize);
        } else if (type.equals("product")) {
            return createProductAggregator(normalize);
        } else if (type.equals("prioritized")) {
            return createPrioritizedAggregator(normalize);
        } else if (type.equals("mean")) {
            return createMeanAggregator(normalize);
        } else if (type.equals("max")) {
            return createMaxAggregator(normalize);
        } else if (type.equals("min")) {
            return createMinAggregator(normalize);
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
                final List<Hit> hits) {

            // We operate only on query layers, i.e., the layers present in the query
            final String[] layers = queryLayers.toArray(new String[queryLayers.size()]);
            final int numLayers = layers.length;

            // If normalization is enabled, obtain max scores for each layer
            double[] maxScores = null;
            if (this.normalize) {
                maxScores = new double[numLayers];
                for (final Hit hit : hits) {
                    for (int i = 0; i < numLayers; ++i) {
                        maxScores[i] = Math.max(maxScores[i], hit.getLayerScore(layers[i]));
                    }
                }
            }

            // Assign aggregate scores, delegating to aggregate(double[])
            final double[] scores = new double[numLayers];
            for (final Hit hit : hits) {
                for (int i = 0; i < numLayers; ++i) {
                    double score = hit.getLayerScore(layers[i]);
                    if (this.normalize && maxScores[i] != 0.0) {
                        score = score / maxScores[i];
                    }
                    scores[i] = score;
                }
                hit.setAggregateScore(aggregate(queryLayers, scores));
            }
        }

        abstract double aggregate(List<String> layers, double[] scores);

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(normalize: " + this.normalize + ")";
        }

    }

    private static final class WeightedAggregator extends SimpleAggregator {

        private final Map<String, Double> layerWeights;

        WeightedAggregator(final boolean normalize, final Map<String, Double> layerWeights) {
            super(normalize);
            this.layerWeights = ImmutableMap.copyOf(layerWeights);
        }

        @Override
        double aggregate(final List<String> layers, final double[] scores) {
            double sum = 0.0;
            double sumWeights = 0.0;
            for (int i = 0; i < scores.length; ++i) {
                final String layer = layers.get(i);
                final Double weight = this.layerWeights.get(layer);
                if (weight != null) {
                    sum += weight * scores[i];
                    sumWeights += weight;
                }
            }
            return sumWeights == 0.0 ? 0.0 : sum / sumWeights;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(normalize: " + this.normalize + ", weights: "
                    + this.layerWeights + ")";
        }

    }

    private static final class SumAggregator extends SimpleAggregator {

        SumAggregator(final boolean normalize) {
            super(normalize);
        }

        @Override
        double aggregate(final List<String> layers, final double[] scores) {
            double sum = 0.0;
            for (int i = 0; i < scores.length; ++i) {
                sum += scores[i];
            }
            return sum / scores.length;
        }

    }

    private static final class ProductAggregator extends SimpleAggregator {

        ProductAggregator(final boolean normalize) {
            super(normalize);
        }

        @Override
        double aggregate(final List<String> layers, final double[] scores) {
            double product = 0.0;
            for (int i = 0; i < scores.length; ++i) {
                product *= scores[i];
            }
            return product;
        }

    }

    private static final class PrioritizedAggregator extends SimpleAggregator {

        // copyright DragoTech :-)

        PrioritizedAggregator(final boolean normalize) {
            super(normalize);
        }

        @Override
        double aggregate(final List<String> layers, final double[] scores) {
            double weight = 1.0;
            double sum = 0.0;
            for (int i = 0; i < scores.length; ++i) {
                if (scores[i] != 0.0) {
                    sum += scores[i] * weight;
                    weight *= scores[i];
                }
            }
            return sum / scores.length; // normalization
        }

    }

    private static final class MeanAggregator extends SimpleAggregator {

        MeanAggregator(final boolean normalize) {
            super(normalize);
        }

        @Override
        double aggregate(final List<String> layers, final double[] scores) {
            int count = 0;
            double sum = 0.0;
            for (int i = 0; i < scores.length; ++i) {
                if (scores[i] != 0.0) {
                    ++count;
                    sum += scores[i];
                }
            }
            return count == 0 ? 0.0 : sum / count;
        }

    }

    private static final class MaxAggregator extends SimpleAggregator {

        MaxAggregator(final boolean normalize) {
            super(normalize);
        }

        @Override
        double aggregate(final List<String> layers, final double[] scores) {
            return Doubles.max(scores);
        }

    }

    private static final class MinAggregator extends SimpleAggregator {

        MinAggregator(final boolean normalize) {
            super(normalize);
        }

        @Override
        double aggregate(final List<String> layers, final double[] scores) {
            return Doubles.min(scores);
        }

    }

}
