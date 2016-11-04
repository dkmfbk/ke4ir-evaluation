package eu.fbk.ke4ir.util;

import java.io.BufferedReader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

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
                    .withHeader("Computes deltas of layer+textual wrt textual only, "
                            + "for all queries in one or more folders")
                    .withLogger(LoggerFactory.getLogger("eu.fbk")).parse(args);

            // Read options
            final Path inputPath = cmd.getOptionValue("i", Path.class);
            final Path outputPath = cmd.getOptionValue("o", Path.class);

            // Allocate a map of writers for output files
            final Map<String, Writer> writers = Maps.newHashMap();

            try {
                // Open output files
                for (final String measure : MEASURES.keySet()) {
                    final Path path = outputPath.resolve("delta." + measure + ".csv");
                    final Writer writer = IO.utf8Writer(IO.buffer(IO.write(path.toString())));
                    writers.put(measure, writer);
                    writer.write(
                            "folder;query;setting;delta_abs;delta_rel;delta_mean;value;textual\n");
                }

                // Process all sub-folders of the specified input folder
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
                                .substring("query-".length());
                        LOGGER.info("Processing query {}", queryId);

                        // Extract all rows for the layer settings effectively available (column 11)
                        final Map<String, String[]> rows = Maps.newHashMap();
                        try (BufferedReader reader = new BufferedReader(
                                IO.utf8Reader(IO.buffer(IO.read(queryPath.toString()))))) {
                            String line;
                            while ((line = reader.readLine()) != null) {
                                final String[] fields = line.split(";");
                                if (fields.length >= 12) {
                                    final String setting = fields[11].replace(' ', ',');
                                    if (setting.contains("textual")
                                            && !rows.containsKey(setting)) {
                                        rows.put(setting, fields);
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

            } finally {
                // Close output files
                for (final Writer writer : writers.values()) {
                    writer.close();
                }
            }

        } catch (final Throwable ex) {
            // Display error information and terminate
            CommandLine.fail(ex);
        }
    }

}
