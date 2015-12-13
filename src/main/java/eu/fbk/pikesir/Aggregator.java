package eu.fbk.pikesir;

import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

import com.google.common.primitives.Doubles;

public abstract class Aggregator {

    public abstract void aggregate(final List<String> allLayers, final List<String> queryLayers,
            final List<Hit> hits);

    public static Aggregator createSumAggregator() {
        return SumAggregator.INSTANCE;
    }

    public static Aggregator createProductAggregator() {
        return ProductAggregator.INSTANCE;
    }

    public static Aggregator createPrioritizedAggregator() {
        return PrioritizedAggregator.INSTANCE;
    }

    public static Aggregator createMeanAggregator() {
        return MeanAggregator.INSTANCE;
    }

    public static Aggregator createMaxAggregator() {
        return MaxAggregator.INSTANCE;
    }

    public static Aggregator createMinAggregator() {
        return MinAggregator.INSTANCE;
    }

    public static Aggregator create(final Path root, final Properties properties, String prefix) {

        // Normalize prefix, ensuring it ends with '.'
        prefix = prefix.endsWith(".") ? prefix : prefix + ".";

        // Select the type of aggregator based on property 'type'
        final String type = properties.getProperty(prefix + "type", "sum").trim().toLowerCase();
        if (type.equals("sum")) {
            return createSumAggregator();
        } else if (type.equals("product")) {
            return createProductAggregator();
        } else if (type.equals("prioritized")) {
            return createPrioritizedAggregator();
        } else if (type.equals("mean")) {
            return createMeanAggregator();
        } else if (type.equals("max")) {
            return createMaxAggregator();
        } else if (type.equals("min")) {
            return createMinAggregator();
        } else {
            throw new IllegalArgumentException("Invalid aggregator type '" + type + "'");
        }
    }

    private static abstract class SimpleAggregator extends Aggregator {

        @Override
        public void aggregate(final List<String> allLayers, final List<String> queryLayers,
                final List<Hit> hits) {

            // We operate only on query layers, i.e., the layers present in the query
            final String[] layers = queryLayers.toArray(new String[queryLayers.size()]);
            final int numLayers = layers.length;

            // Obtain max scores for each layer, required for normalization
            final double[] maxScores = new double[numLayers];
            for (final Hit hit : hits) {
                for (int i = 0; i < numLayers; ++i) {
                    maxScores[i] = Math.max(maxScores[i], hit.getLayerScore(layers[i]));
                }
            }

            // Assign aggregate scores, delegating to aggregate(double[])
            final double[] scores = new double[numLayers];
            for (final Hit hit : hits) {
                for (int i = 0; i < numLayers; ++i) {
                    final double maxScore = maxScores[i];
                    scores[i] = maxScores[i] == 0.0 ? 0.0 : hit.getLayerScore(layers[i])
                            / maxScore;
                }
                hit.setAggregateScore(aggregate(scores));
            }
        }

        abstract double aggregate(double[] scores);

    }

    private static final class SumAggregator extends SimpleAggregator {

        static final SumAggregator INSTANCE = new SumAggregator();

        @Override
        double aggregate(final double[] scores) {
            double sum = 0.0;
            for (int i = 0; i < scores.length; ++i) {
                sum += scores[i];
            }
            return sum / scores.length;
        }

    }

    private static final class ProductAggregator extends SimpleAggregator {

        static final ProductAggregator INSTANCE = new ProductAggregator();

        @Override
        double aggregate(final double[] scores) {
            double product = 0.0;
            for (int i = 0; i < scores.length; ++i) {
                product *= scores[i];
            }
            return product;
        }

    }

    private static final class PrioritizedAggregator extends SimpleAggregator {

        // copyright DragoTech :-)

        static final PrioritizedAggregator INSTANCE = new PrioritizedAggregator();

        @Override
        double aggregate(final double[] scores) {
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

        static final MeanAggregator INSTANCE = new MeanAggregator();

        @Override
        double aggregate(final double[] scores) {
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

        static final MaxAggregator INSTANCE = new MaxAggregator();

        @Override
        double aggregate(final double[] scores) {
            return Doubles.max(scores);
        }

    }

    private static final class MinAggregator extends SimpleAggregator {

        static final MinAggregator INSTANCE = new MinAggregator();

        @Override
        double aggregate(final double[] scores) {
            return Doubles.min(scores);
        }

    }

}
