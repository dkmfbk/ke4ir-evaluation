package eu.fbk.shell.pikesir.main;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import com.dragotech.tools.FileManager;
import com.dragotech.tools.FileManager.Mode;

import eu.fbk.shell.pikesir.datastructure.QueryJudgments;
import eu.fbk.shell.pikesir.datastructure.SemanticStructure;
import eu.fbk.shell.pikesir.indexer.IndexCreator;
import eu.fbk.shell.pikesir.searcher.Searcher;
import eu.fbk.shell.pikesir.searcher.SearcherQueryRanks;

public class MainPIKESIR {
  
  public static void main(String[] args) {
    try {
      Properties prp = new Properties();
      InputStream iS = new FileInputStream(new File("").getAbsolutePath() + "/" + "pikesir.properties");
      prp.load(iS);
      
      int indexFlag = Integer.valueOf(prp.getProperty("pikesir.index"));
      int searchFlag = Integer.valueOf(prp.getProperty("pikesir.search"));
      String documentsFilePath = prp.getProperty("pikesir.documentspath");
      String queriesFilePath = prp.getProperty("pikesir.queriespath");
      String relevanceFilePath = prp.getProperty("pikesir.relevancepath");
      
      
      /* Create index */
      if(indexFlag == 1) {
        //FileManager fm = new FileManager("/home/drago/Documents/research/papers/conferences/ESWC2016SemanticIR/data_v3/documents.tsv", Mode.READ);
        FileManager fm = new FileManager(documentsFilePath, Mode.READ);
        HashMap<String, SemanticStructure> docs = new HashMap<String, SemanticStructure>();
        String t = fm.read();
        while(t != null) {
          String[] tokens = t.split("\t", 2);
          SemanticStructure d = docs.get(tokens[0]);
          if(d == null) {
            d = new SemanticStructure(tokens[0]);
          }
          d.addTerm(tokens[1]);
          docs.put(tokens[0], d);
          t = fm.read();
        }
        IndexCreator ic = new IndexCreator(prp);
        ic.createIndex(docs);  
      }
      
      
      /* Performs queries */
      if(searchFlag == 1) {
        //FileManager fmR = new FileManager("/home/drago/Documents/research/papers/conferences/ESWC2016SemanticIR/relevance_v1.txt", Mode.READ);
        FileManager fmR = new FileManager(relevanceFilePath, Mode.READ);
        HashMap<String, QueryJudgments> relevance = new HashMap<String, QueryJudgments>();
        String r = fmR.read();
        while(r != null) {
          String[] relTokens = r.split("\t");
          String[] js = relTokens[2].split(",");
          QueryJudgments qj = new QueryJudgments(relTokens[0]);
          for(String rj: js) {
            String[] values = rj.split(":");
            Integer relScore = 5 - Integer.valueOf(values[1]);
            if(relScore == 0) {
              continue;
            }
            //qj.addJudge(Integer.valueOf(values[1]), values[0]);
            qj.addJudge(relScore, values[0]);
          }
          relevance.put(relTokens[0], qj);
          r = fmR.read();
        }
        
        
        //FileManager fm = new FileManager("/home/drago/Documents/research/papers/conferences/ESWC2016SemanticIR/data_v3/queries.tsv", Mode.READ);
        FileManager fm = new FileManager(queriesFilePath, Mode.READ);
        HashMap<String, SemanticStructure> queries = new HashMap<String, SemanticStructure>();
        String t = fm.read();
        while(t != null) {
          String[] tokens = t.split("\t", 2);
          SemanticStructure d = queries.get(tokens[0]);
          if(d == null) {
            d = new SemanticStructure(tokens[0]);
          }
          d.addTerm(tokens[1]);
          queries.put(tokens[0], d);
          t = fm.read();
        }
        Searcher s = new Searcher(prp);
        s.search(queries, relevance);
        
        SearcherQueryRanks sq = new SearcherQueryRanks(prp);
        sq.search(queries, relevance);
      }
      
    } catch(Exception e) {
      e.printStackTrace();
    }
    
  }
}
