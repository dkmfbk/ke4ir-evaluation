package eu.fbk.ke4ir.util;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApproximateRandomization {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApproximateRandomization.class);

    public static double test(final int iterations, final double[] a, final double b[]) {

        double bs = calculateScore(a);
        double ps = calculateScore(b);
        final double d = Math.abs(ps - bs);

        double p = 0;
        double mean = 0;
        double variance = 0;
        double sum = 0;
        double ssum = 0;

        // c - number of times that the pseudostatistic is greater or equal to the true statistic
        int c = 0;
        for (int i = 0; i < iterations; i++) {
            final double[] x = new double[a.length];
            final double[] y = new double[b.length];

            System.arraycopy(a, 0, x, 0, a.length);
            System.arraycopy(b, 0, y, 0, b.length);

            swap(x, y, new Random(i * 123));
            bs = calculateScore(x);
            ps = calculateScore(y);

            final double di = Math.abs(ps - bs);
            sum += di;
            ssum += Math.pow(di, 2);

            if (di >= d) {
                c++;
            }
        }

        mean = sum / iterations;
        variance = (iterations * ssum - Math.pow(sum, 2)) / iterations * (iterations - 1);

        p = (double) (c + 1) / (iterations + 1);

        LOGGER.debug("Mean: " + mean + ", " + Math.sqrt(variance));
        LOGGER.debug(p + " = (" + c + " + 1) / (" + iterations + " + 1)");

        return p;
    }

    private static void swap(final double[] y, final double[] z, final Random rdm) {
        for (int i = 0; i < y.length; i++) {
            final double p = rdm.nextDouble();
            if (p < 0.5) {
                final double t = y[i];
                y[i] = z[i];
                z[i] = t;
            }
        }
    }

    static double calculateScore(final double[] tmp) {
        double sum = 0.0;
        for (final double v : tmp) {
            sum += v;
        }
        return sum / tmp.length;
    }

}
