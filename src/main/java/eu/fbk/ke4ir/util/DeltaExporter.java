package eu.fbk.ke4ir.util;

import java.io.BufferedReader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.primitives.Doubles;

import org.apache.commons.math3.stat.inference.TTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.rdfpro.util.IO;

public class DeltaExporter {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeltaExporter.class);

    private static final Map<String, Integer> MEASURES = ImmutableMap.<String, Integer>builder()
            .put("p1", 1).put("p3", 2).put("p5", 3).put("p10", 4).put("mrr", 5).put("ndcg", 6)
            .put("ndcg10", 7).put("map", 8).put("map10", 9).build();

    public static void main(final String... args) {
        try {
            // Parse command line
            final CommandLine cmd = CommandLine.parser().withName("delta-exporter")
                    .withOption("i", "input",
                            "the input folder (must contain subfolders for each analyzed dataset)",
                            "PATH", CommandLine.Type.DIRECTORY_EXISTING, true, false, true)
                    .withOption("o", "output", "the output folder", "PATH",
                            CommandLine.Type.DIRECTORY_EXISTING, true, false, true)
                    .withOption("a", "available", "base analysis on available layers only")
                    .withHeader("Computes deltas of layer+textual wrt textual only, "
                            + "for all queries in one or more folders")
                    .withLogger(LoggerFactory.getLogger("eu.fbk")).parse(args);

            // Read options
            final Path inputPath = cmd.getOptionValue("i", Path.class);
            final Path outputPath = cmd.getOptionValue("o", Path.class);
            final boolean useAvailableLayers = cmd.hasOption("a");

            // Allocate a map of writers for output files
            final Map<String, Writer> writers = Maps.newHashMap();
            Writer pvalueWriter = null;

            try {
                // Open output files
                for (final String measure : MEASURES.keySet()) {
                    final Writer writer = IO.utf8Writer(IO.buffer(
                            IO.write(outputPath.resolve("delta." + measure + ".csv").toString())));
                    writers.put(measure, writer);
                    writer.write(
                            "folder;query;setting;delta_abs;delta_rel;delta_mean;value;textual\n");
                    pvalueWriter = IO.utf8Writer(IO
                            .buffer(IO.write(outputPath.resolve("delta.pvalues.csv").toString())));
                    pvalueWriter.write("measure;folder;setting;num_queries;avg_value;avg_textual;"
                            + "t;pvalue_different;pvalue_better;pvalue_worse\n");
                }

                // Process all sub-folders of the specified input folder
                final Table<String, String, Map<String, String[]>> data = HashBasedTable.create();
                final Set<String> settings = Sets.newHashSet();
                for (final Path folderPath : Files.list(inputPath)
                        .filter(p -> Files.isDirectory(p)).collect(Collectors.toList())) {

                    // Compute and log folder ID
                    final String folderId = folderPath.getFileName().toString();
                    LOGGER.info("Processing folder {}", folderId);

                    // Process all query files in the current folder
                    for (final Path queryPath : Files.list(folderPath)
                            .filter(p -> p.getFileName().toString().startsWith("query-"))
                            .collect(Collectors.toList())) {

                        // Compute and log query ID
                        final String queryId = queryPath.getFileName().toString()
                                .substring("query-".length()).replace(".csv", "");
                        LOGGER.info("Processing query {}", queryId);

                        // Extract all rows for the layer settings effectively available (column 11)
                        final Map<String, String[]> rows = Maps.newHashMap();
                        data.put(folderId, queryId, rows);
                        try (BufferedReader reader = new BufferedReader(
                                IO.utf8Reader(IO.buffer(IO.read(queryPath.toString()))))) {
                            String line;
                            while ((line = reader.readLine()) != null) {
                                final String[] fields = line.split(";");
                                if (!"setting".equals(fields[0]) && fields[0].contains("textual")
                                        && fields.length >= 12) {
                                    final String setting = fields[0];
                                    if (useAvailableLayers) {
                                        final String available = fields[11].replace(' ', ',');
                                        if (setting.equals(available)
                                                && !rows.containsKey(setting)) {
                                            rows.put(setting, fields);
                                            settings.add(setting);
                                        }
                                    } else {
                                        rows.put(setting, fields);
                                        settings.add(setting);
                                    }
                                }
                            }
                        }

                        // Retrieve the row for the textual setting
                        final String[] textualRow = rows.get("textual");
                        if (textualRow == null) {
                            LOGGER.warn("No data for 'textual' setting in file " + queryPath);
                            continue;
                        }

                        // Compute deltas and update output delta files
                        for (final Entry<String, String[]> entry : rows.entrySet()) {
                            final String setting = entry.getKey();
                            final String[] row = entry.getValue();
                            if (!"textual".equals(setting)) {
                                for (final String measure : MEASURES.keySet()) {
                                    final int index = MEASURES.get(measure);
                                    final double value = Double.parseDouble(row[index]);
                                    final double textual = Double.parseDouble(textualRow[index]);
                                    final double mean = 0.5 * (value + textual);
                                    final double deltaAbs = value - textual;
                                    final double deltaMean = mean == 0.0 ? 0.0 : deltaAbs / mean;
                                    final double deltaRel = textual == 0.0 ? Double.NaN
                                            : deltaAbs / textual;
                                    final Writer writer = writers.get(measure);
                                    writer.write(folderId + ";" + queryId + ";" + setting + ";"
                                            + deltaAbs + ";" + deltaRel + ";" + deltaMean + ";"
                                            + value + ";" + textual + "\n");
                                }
                            }
                        }
                    }
                }

                // Compute and emit p-values
                for (final String measure : MEASURES.keySet()) {
                    final int index = MEASURES.get(measure);
                    final List<String> folderFilters = Lists.newArrayList(data.rowKeySet());
                    folderFilters.add("all");
                    for (final String folderFilter : Ordering.natural()
                            .sortedCopy(folderFilters)) {
                        for (final String setting : settings) {
                            final List<Double> system = Lists.newArrayList();
                            final List<Double> textual = Lists.newArrayList();
                            for (final String folder : data.rowKeySet()) {
                                if (!folderFilter.equals(folder) && !(folderFilter.equals("all")
                                        && !folder.contains("_desc"))) {
                                    continue;
                                }
                                for (final Map<String, String[]> rows : data.row(folder)
                                        .values()) {
                                    final String[] textualRow = rows.get("textual");
                                    final String[] systemRow = rows.get(setting);
                                    if (textualRow == null || systemRow == null) {
                                        continue;
                                    }
                                    textual.add(Double.valueOf(textualRow[index]));
                                    system.add(Double.valueOf(systemRow[index]));
                                }
                            }
                            final double[] systemVals = Doubles.toArray(system);
                            final double[] textualVals = Doubles.toArray(textual);
                            final int numQueries = system.size();
                            final double systemAvg = system.stream().collect(
                                    Collectors.summingDouble(Double::doubleValue)) / numQueries;
                            final double textualAvg = textual.stream().collect(
                                    Collectors.summingDouble(Double::doubleValue)) / numQueries;
                            final double t = new TTest().pairedT(systemVals, textualVals);
                            final double pvalueDifferent = new TTest().pairedTTest(systemVals,
                                    textualVals);
                            final double pvalueBetter = t > 0 ? pvalueDifferent / 2
                                    : 1.0 - pvalueDifferent / 2;
                            final double pvalueWorse = t < 0 ? pvalueDifferent / 2
                                    : 1.0 - pvalueDifferent / 2;
                            pvalueWriter.write(measure + ";" + folderFilter + ";" + setting + ";"
                                    + numQueries + ";" + systemAvg + ";" + textualAvg + ";" + t
                                    + ";" + pvalueDifferent + ";" + pvalueBetter + ";"
                                    + pvalueWorse + "\n");
                        }
                    }
                }

            } finally {
                // Close output files
                for (final Writer writer : writers.values()) {
                    writer.close();
                }
                pvalueWriter.close();
            }

        } catch (final Throwable ex) {
            // Display error information and terminate
            CommandLine.fail(ex);
        }
    }

}
