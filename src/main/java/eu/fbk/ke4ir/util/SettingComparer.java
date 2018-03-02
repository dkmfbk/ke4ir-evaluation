package eu.fbk.ke4ir.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import com.google.common.base.Splitter;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

import org.apache.commons.math3.stat.inference.TTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.ke4ir.util.CommandLine.Type;

public class SettingComparer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SettingComparer.class);

    public static void main(final String... args) {
        try {
            // Parse command line arguments
            final CommandLine cmd = CommandLine.parser()
                    .withHeader("Compares the performances of two datasets, "
                            + "performing statistical significance tests")
                    .withOption("b", "baseline",
                            "the settings FILE for the baseline (e.g., setting-textual.csv)",
                            "FILE", Type.FILE_EXISTING, true, false, true)
                    .withOption("s", "system",
                            "the settings FILE for the compared system "
                                    + "(e.g., setting-textual-uri-type-frame-time.csv)",
                            "FILE", Type.FILE_EXISTING, true, false, true)
                    .withOption("B", "baseline-label", "the LABEL for the baseline", "LABEL",
                            Type.STRING, true, false, false)
                    .withOption("S", "system-label", "the LABEL for the system", "LABEL",
                            Type.STRING, true, false, false)
                    .withOption("m", "measures", "restrict comparison to supplied MEASURES",
                            "MEASURES", Type.STRING, true, false, false)
                    .withOption("l", "latex", "emit latex report") //
                    .withOption("t", "two-tailed",
                            "if set, two-tailed significance tests are used")
                    .parse(args);

            // Read command line options
            final Path baselinePath = cmd.getOptionValue("b", Path.class);
            final Path systemPath = cmd.getOptionValue("s", Path.class);
            final String baselineLabel = cmd.getOptionValue("B", String.class, "baseline");
            final String systemLabel = cmd.getOptionValue("S", String.class, "system");
            final String measuresOpt = cmd.getOptionValue("m", String.class);
            final boolean latex = cmd.hasOption("l");
            final boolean twoTailed = cmd.hasOption("t");

            final Table<String, String, Double> baselineMeasures = read(baselinePath);
            final Table<String, String, Double> systemMeasures = read(systemPath);

            String[] measures;
            if (measuresOpt != null) {
                measures = Iterables.toArray(
                        Splitter.onPattern("[,;\\s]").trimResults().split(measuresOpt),
                        String.class);
            } else {
                measures = Iterables.toArray(Sets.intersection(baselineMeasures.rowKeySet(),
                        systemMeasures.rowKeySet()), String.class);
                Arrays.sort(measures);
            }

            final String[][] table = new String[6][];
            for (int i = 0; i < table.length; ++i) {
                table[i] = new String[measures.length + 1];
                Arrays.fill(table[i], "");
            }
            table[0][0] = format("Approach/System", true, latex);
            table[1][0] = baselineLabel;
            table[2][0] = systemLabel;
            table[3][0] = baselineLabel + " vs. " + systemLabel;
            table[4][0] = (latex ? "$p$" : "p") + "-value (paired t-test)";
            table[5][0] = (latex ? "$p$" : "p") + "-value (approx. random.)";

            for (int i = 0; i < measures.length; ++i) {
                final String measure = measures[i];

                final Set<String> baselineQueries = baselineMeasures.row(measure).keySet();
                final Set<String> systemQueries = systemMeasures.row(measure).keySet();
                final List<String> queries = Ordering.natural()
                        .sortedCopy(Sets.intersection(baselineQueries, systemQueries));

                final double[] baselineScores = new double[queries.size()];
                final double[] systemScores = new double[queries.size()];
                for (int j = 0; j < queries.size(); ++j) {
                    final String query = queries.get(j);
                    baselineScores[j] = baselineMeasures.get(measure, query);
                    systemScores[j] = systemMeasures.get(measure, query);
                }

                int count = 0;
                for (int k = 0; k < baselineScores.length; ++k) {
                    count += !Double.isNaN(baselineScores[k]) && //
                            !Double.isNaN(systemScores[k]) ? 1 : 0;
                }
                double[] b = baselineScores;
                double[] s = systemScores;
                if (count < baselineScores.length) {
                    b = new double[count];
                    s = new double[count];
                    int j = 0;
                    for (int k = 0; k < baselineScores.length; ++k) {
                        if (!Double.isNaN(baselineScores[k]) && !Double.isNaN(systemScores[k])) {
                            b[j] = baselineScores[k];
                            s[j] = systemScores[k];
                            ++j;
                        }
                    }
                }

                final double baselineMean = mean(b);
                final double systemMean = mean(s);
                final double improvement = (systemMean - baselineMean) / baselineMean;
                final double pvalueTTest = pvalueTTest(b, s, twoTailed);
                final double pvalueAR = pvalueAR(b, s, twoTailed);

                if (pvalueTTest <= 0.05 != pvalueAR <= 0.05) {
                    LOGGER.warn("Inconsistent statistical significance results (pvalue <= 0.05) "
                            + "for paired t-test / approx. random. on measure " + measure);
                }

                final int col = i + 1;
                table[0][col] = format(measure, true, latex);
                table[1][col] = format(String.format("%.3f", baselineMean),
                        baselineMean > systemMean, latex);
                table[2][col] = format(String.format("%.3f", systemMean),
                        systemMean > baselineMean, latex);
                table[3][col] = format(
                        String.format("%.2f", improvement * 100) + (latex ? "\\%" : "%"), false,
                        latex);
                table[4][col] = format(String.format("%.3f", pvalueTTest), pvalueTTest <= 0.05,
                        latex);
                table[5][col] = format(String.format("%.3f", pvalueAR), pvalueAR <= 0.05, latex);
            }

            final int nrows = table.length;
            final int ncols = table[0].length;
            final int[] widths = new int[ncols];
            for (int i = 0; i < ncols; ++i) {
                for (int j = 0; j < nrows; ++j) {
                    widths[i] = Math.max(widths[i], table[j][i].length());
                }
            }

            final String colSep = latex ? " & " : "   ";
            final String rowSep = latex ? " \\\\\\hline\n" : "\n";
            final StringBuilder out = new StringBuilder();
            for (int i = 0; i < nrows; ++i) {
                for (int j = 0; j < ncols; ++j) {
                    if (j > 0) {
                        out.append(colSep);
                    }
                    out.append(String.format("%" + widths[j] + "s", table[i][j]));
                }
                out.append(rowSep);
            }
            LOGGER.info("Comparison:\n{}", out);

        } catch (final Throwable ex) {
            // Report error and terminate
            CommandLine.fail(ex);
        }
    }

    private static String format(final String text, final boolean bold, final boolean latex) {
        if (latex) {
            return bold ? "\\textbf{" + text + "}" : "        " + text + " ";
        } else {
            return bold ? "*" + text + "*" : " " + text + " ";
        }
    }

    private static double mean(final double[] values) {
        double sum = 0.0;
        for (final double value : values) {
            sum += value;
        }
        return sum / values.length;
    }

    private static double pvalueTTest(final double[] baseline, final double[] system,
            final boolean twoTailed) {
        final double pvalue = new TTest().pairedTTest(baseline, system);
        return twoTailed ? pvalue : pvalue / 2.0;
    }

    private static double pvalueAR(final double[] baseline, final double[] system,
            final boolean twoTailed) {
        final double pvalue = ApproximateRandomization.test(100000, baseline, system);
        return twoTailed ? pvalue : pvalue / 2;
    }

    private static Table<String, String, Double> read(final Path file) throws IOException {

        final Table<String, String, Double> measures = HashBasedTable.create();
        final Set<String> ignoredColumns = Sets.newHashSet();
        String[] header = null;

        for (String line : Files.readAllLines(file)) {
            line = line.trim();
            if (line.isEmpty()) {
                continue;
            }
            final String[] tokens = line.split("[;,\t]");
            if (header == null) {
                header = tokens;
                continue;
            }
            final String query = tokens[0].trim();
            for (int i = 1; i < tokens.length; ++i) {
                final String measure = header[i];
                try {
                    final double value = Double.parseDouble(tokens[i].trim());
                    measures.put(measure, query, value);
                } catch (final NumberFormatException ex) {
                    ignoredColumns.add(measure);
                }
            }
        }

        for (final String ignoredColumn : ignoredColumns) {
            measures.rowMap().remove(ignoredColumn);
        }
        LOGGER.info("Read {} measures from {}", measures.rowKeySet().size(), file);
        return measures;
    }

}
