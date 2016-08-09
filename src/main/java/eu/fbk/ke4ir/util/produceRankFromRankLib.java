package eu.fbk.ke4ir.util;

import com.google.common.collect.Maps;
import eu.fbk.rdfpro.util.IO;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by marcorospocher on 04/08/16.
 */
public class produceRankFromRankLib {






    public static void main(final String... args) throws IOException {


        final CommandLine cmd = CommandLine
                .parser()
                .withName("rank the reranker")
                .withOption("i", "input", "input",
                        "PATH", CommandLine.Type.FILE_EXISTING, true, false, false)
                .withOption("r", "rank", "rank",
                        "PATH", CommandLine.Type.FILE_EXISTING, true, false, false)
                .withOption("o", "output", "output",
                        "PATH", CommandLine.Type.FILE_EXISTING, true, false, false)
                .withOption("s", "score", "score").parse(args);

        boolean withScores = cmd.hasOption("s");



        final Map<String, Map<String, String>> mapIDdocs = Maps.newHashMap();
        String prev_query="";
        int i=0;
        for (final String line : Files.readAllLines(cmd.getOptionValue("r",  Path.class))) {
            final String[] tokens = line.split("[\\s+,;]+");
            if (!tokens[0].equals("#")) {
                Map<String, String> map = Maps.newHashMap();
                String query = tokens[1].replace("qid:", "q");

                if (!query.equals(prev_query)) {
                    i = 0;
                    prev_query = query;
                } else i++;

                if (mapIDdocs.containsKey(query)) {
                    map = mapIDdocs.get(query);
                }
                map.put(Integer.toString(i), tokens[tokens.length - 1]);
                mapIDdocs.put(query, map);
            }
        }


        final Map<String, Map<String, Double>> rank = Maps.newHashMap();
        for (final String line : Files.readAllLines(cmd.getOptionValue("i",  Path.class))) {
            final String[] tokens = line.split("[\\s+,;]+");
            Map<String, Double> map = Maps.newHashMap();
            String query = "q"+tokens[0];
            if (rank.containsKey(query)){
                map=rank.get(query);
            }
            String doc = mapIDdocs.get(query).get(tokens[1]);
            //System.out.println(doc);
            map.put(doc,Double.parseDouble(tokens[2]));
            rank.put(query, map);
        }








        try (Writer writer = IO.utf8Writer(IO.buffer(IO.write(cmd.getOptionValue("o",  Path.class)
                .toAbsolutePath().toString())))) {

            TreeSet<String> ts = new TreeSet<String>(rank.keySet());

            for(String query : ts){
                String line = query+"\t";
                //rank.put(query,sortByValue(rank.get(query)));

                final Comparator<Map.Entry<String, Double>> byValue =
                        Comparator.comparing(Map.Entry::getValue, Comparator.reverseOrder());
                final Comparator<Map.Entry<String, Double>> byKey =
                        Comparator.comparing(Map.Entry::getKey);

                final List<String> orderedDocs = rank.get(query)
                        .entrySet().stream().sequential()
                        .sorted(byValue.thenComparing(byKey))
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toList());

                for(String doc : orderedDocs)
                    if (withScores) line +=doc+" ["+rank.get(query).get(doc)+"]"+"\t";
                    else  line +=doc+"\t";
                line=line.trim();
                //System.out.println(line);
                writer.append(line+"\n");
            }


        }


        //final String[] line = {q.getKey() + "\t"};



        //System.out.println("Ciao");

    }

    public static Map<String, Double> sortByValue( Map<String, Double>  map )
    {


        Map<String, Double>  result = new LinkedHashMap<>();
        Stream<Map.Entry<String, Double> > st = map.entrySet().stream();

        st.sorted( new Comparator<Map.Entry<String, Double> >() {

        public int compare(Map.Entry<String, Double>  a, Map.Entry<String, Double>  b) {
            int result = -1 * Double.compare(a.getValue(),b.getValue());
            if (result == 0) {
                result = a.getKey().compareTo(b.getKey());
            }
            return result;
        }

    }).forEachOrdered( e -> result.put(e.getKey(), e.getValue()) );

        return result;
    }

}
