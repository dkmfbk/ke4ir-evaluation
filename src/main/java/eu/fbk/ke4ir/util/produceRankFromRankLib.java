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

        final Map<String, Map<String, Double>> rank = Maps.newHashMap();
        for (final String line : Files.readAllLines(cmd.getOptionValue("i",  Path.class))) {
            final String[] tokens = line.split("[\\s+,;]+");
            Map<String, Double> map = Maps.newHashMap();

            if (rank.containsKey(tokens[0])){
                map=rank.get(tokens[0]);
            }
            map.put(tokens[1],Double.parseDouble(tokens[2]));
            rank.put(tokens[0], map);
        }



        final Map<String, Map<String, String>> mapIDdocs = Maps.newHashMap();
        String prev_query="";
        int i=0;
        for (final String line : Files.readAllLines(cmd.getOptionValue("r",  Path.class))) {
            final String[] tokens = line.split("[\\s+,;]+");
            if (!tokens[0].equals("#")) {
                Map<String, String> map = Maps.newHashMap();
                String query = tokens[1].replace("qid:", "");

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





        try (Writer writer = IO.utf8Writer(IO.buffer(IO.write(cmd.getOptionValue("o",  Path.class)
                .toAbsolutePath().toString())))) {

            TreeSet<String> ts = new TreeSet<String>(rank.keySet());

            for(String query : ts){
                String line = "q"+query+"\t";
                rank.put(query,sortByValue(rank.get(query)));
                for(String doc : rank.get(query).keySet())
                    if (withScores) line +=mapIDdocs.get(query).get(doc)+" ["+rank.get(query).get(doc)+"]"+"\t";
                    else  line +=mapIDdocs.get(query).get(doc)+"\t";
                line=line.trim();
                //System.out.println(line);
                writer.append(line+"\n");
            }


        }


        //final String[] line = {q.getKey() + "\t"};



        //System.out.println("Ciao");

    }

    public static <K extends Comparable<? super K>, V extends Comparable<? super V>> Map<K, V>
    sortByValue( Map<K, V> map )
    {


        Map<K, V> result = new LinkedHashMap<>();
        Stream<Map.Entry<K, V>> st = map.entrySet().stream();

        st.sorted( new Comparator<Map.Entry<K, V>>() {

        public int compare(Map.Entry<K, V> a, Map.Entry<K, V> b) {
            int cmp1 = -1*a.getValue().compareTo(b.getValue());
            if (cmp1 != 0) {
                return cmp1;
            } else {
                return a.getKey().compareTo(b.getKey());
            }
        }

    }).forEachOrdered( e -> result.put(e.getKey(), e.getValue()) );

        return result;
    }

}
