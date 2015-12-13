package eu.fbk.shell.pikesir.searcher;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import com.dragotech.tools.FileManager;
import com.dragotech.tools.FileManager.Mode;

import eu.fbk.shell.pikesir.datastructure.QueryJudgments;
import eu.fbk.shell.pikesir.datastructure.SemanticStructure;
import eu.fbk.shell.pikesir.indexer.IndexCreator;


public class SearcherQueryRanks {
  
  private Properties prp;
  private String outPath;
  private String mode;
  
  public SearcherQueryRanks(Properties p) {
    this.prp = p;
    this.outPath = prp.getProperty("pikesir.output.queryranks");
    this.mode = prp.getProperty("pikesir.mode");
  }
    
  
  public void search(HashMap<String, SemanticStructure> queries, HashMap<String, QueryJudgments> relevance) {
    try {
      SearchManager sm = new SearchManager(this.prp);
      sm.openIndex("pikesir", IndexCreator.createAnalyzer());
      
    
      /*
       * Quelli qui sotto sono i vari layers.
       * Ogni array contiene la lista dei campi contenuti all'interno dei file .tsv.
       * Se vuoi aggiungere un layer... basta creare un altro array...
       * Dalla riga 115 (circa) in poi, ci sono i controlli sull'appartenenza di un campo ad un layer.
       */
      //String[] layerTextual = {"lemma.related", "lemma.synonym", "lemma.text", "stem.related", "stem.synonym", "stem.text"};
      //String[] layerTextual = {"lemma.text", "stem.text"};
      //String[] layerTextual = {"lemma.synonym", "lemma.text", "stem.subword", "stem.synonym", "stem.text"};
      String[] layerTextual = {"lemma.text", "stem.text"};
      
      String[] layerPredicate = {"predicate.frb", "predicate.nb", "predicate.pb"};
      //String[] layerPredicate = {"predicate.frb"};
      
      String[] layerRole = {"role.frb", "role.nb", "role.pb"};
      //String[] layerRole = {"role.frb"};
      
      String[] layerSynset = {"synset.hypernym", "synset.hypernym.amb", "synset.related", "synset.specific", "synset.specific.amb"};
      //String[] layerSynset = {"synset.related", "synset.specific", "synset.specific.amb"};
      
      String[] layerType = {"type.yago", "type.sumo"};
      //String[] layerType = {"type.yago"};
      
      String[] layerURI = {"uri.custom", "uri.dbpedia"};
      
      String[] layerConcept = {"concept"};


      /*
       * Lista delle etichette che compaiono nella prima riga dell'excel.
       * L'ordine delle etichette e' lo stesso dei layer elencati qui sopra.
       */
      String[] fieldsSet = {"textual", "predicate", "role", "synset", "type", "uri", "concept"};
      
      
      
      
        
      double prec1 = 0.0;
      double rr = 0.0;
      double prec3 = 0.0;
      double prec5 = 0.0;
      double prec10 = 0.0;
      double ndcg = 0.0;
      double ndcg10 = 0.0;
      double map = 0.0;
      double map10 = 0.0;
      
      Iterator<String> itQuery = queries.keySet().iterator();
      while(itQuery.hasNext()) {
        String idQuery = itQuery.next();
        //if(idQuery.compareTo("q01") != 0) continue;
        SemanticStructure q = queries.get(idQuery);
        QueryJudgments relDocs = relevance.get(idQuery);
        ArrayList<String> relDocsList = relDocs.getRelDocsList();
       
        StringBuffer queryTextual = new StringBuffer("");
        StringBuffer queryPredicate = new StringBuffer("");
        StringBuffer queryRole = new StringBuffer("");
        StringBuffer querySynset = new StringBuffer("");
        StringBuffer queryType = new StringBuffer("");
        StringBuffer queryUri = new StringBuffer("");
        StringBuffer queryConcept = new StringBuffer("");
        
        ArrayList<String> t = q.getTerms();
        for(String s: t) {
          String[] tokens = s.split("\t");
          
          if(Arrays.asList(layerTextual).contains(tokens[0])) {
            if(tokens[0].startsWith("lemma")) {
              queryTextual.append("lemma.text:\"" + tokens[1].toLowerCase() + "\" OR ");
            } else if(tokens[0].startsWith("stem")) {
              queryTextual.append("stem.text:\"" + tokens[1].toLowerCase() + "\" OR ");
            }
            
            // From this point, the index value of the array "fieldsFlag" has been decreased by one due to the join of the two
            // arrays "lemma" and "stem"
          } else if(Arrays.asList(layerPredicate).contains(tokens[0])) {
            if(this.mode.compareTo("A") == 0) {
              queryPredicate.append(tokens[0] + ".all:\"" + tokens[1].toLowerCase() + "\" OR ");
            } else if(this.mode.compareTo("B") == 0) {
              if(s.indexOf("inherited") == -1) {
                queryPredicate.append(tokens[0] + ".all:\"" + tokens[1].toLowerCase() + "\" OR ");
              }
            }
          } 
          
          else if(Arrays.asList(layerRole).contains(tokens[0])) {
            if(this.mode.compareTo("A") == 0) {
              queryRole.append(tokens[0] + ".all:\"" + tokens[1].toLowerCase() + "\" OR ");
            } else if(this.mode.compareTo("B") == 0) {
              if(s.indexOf("inherited") == -1) {
                queryRole.append(tokens[0] + ".all:\"" + tokens[1].toLowerCase() + "\" OR ");
              }
            }
          } 
          
          else if(Arrays.asList(layerSynset).contains(tokens[0])) {
            if(this.mode.compareTo("A") == 0) {
              querySynset.append(tokens[0] + ".all:\"" + tokens[1].toLowerCase() + "\" OR ");
            } else if(this.mode.compareTo("B") == 0) {
              if(s.indexOf("inherited") == -1 && s.indexOf("certain=false") == -1 && s.indexOf("len") == -1) {
                querySynset.append(tokens[0] + ".all:\"" + tokens[1].toLowerCase() + "\" OR ");
              }
            }
          } 
          
          else if(Arrays.asList(layerType).contains(tokens[0])) {
            if(this.mode.compareTo("A") == 0) {
              queryType.append(tokens[0] + ".all:\"" + tokens[1].toLowerCase() + "\" OR ");
            } else if(this.mode.compareTo("B") == 0) {
              if(s.indexOf("inherited") == -1) {
                queryType.append(tokens[0] + ".all:\"" + tokens[1].toLowerCase() + "\" OR ");
              }
            }
          } 
          
          else if(Arrays.asList(layerURI).contains(tokens[0])) {
            if(this.mode.compareTo("A") == 0) {
              queryUri.append(tokens[0] + ".all:\"" + tokens[1].toLowerCase() + "\" OR ");
            } else if(this.mode.compareTo("B") == 0) {
              if(s.indexOf("inherited") == -1) {
                queryUri.append(tokens[0] + ".all:\"" + tokens[1].toLowerCase() + "\" OR ");
              }
            }
          } 
          
          else if(Arrays.asList(layerConcept).contains(tokens[0])) {
            if(this.mode.compareTo("A") == 0) {
              queryConcept.append(tokens[0] + ".all:\"" + tokens[1].toLowerCase() + "\" OR ");
            } else if(this.mode.compareTo("B") == 0) {
              if(s.indexOf("inherited") == -1) {
                queryConcept.append(tokens[0] + ".all:\"" + tokens[1].toLowerCase() + "\" OR ");
              }
            }
          }
        }
        

        
        String[] queriesSet = {queryTextual.toString(), 
                               queryPredicate.toString(), 
                               queryRole.toString(), 
                               querySynset.toString(), 
                               queryType.toString(), 
                               queryUri.toString(), 
                               queryConcept.toString()};
        
        String[][] scores = new String[7][50];
        
        for(int j = 0; j < fieldsSet.length; j++) {
          String query = queriesSet[j];
          if(query.length() == 0) {
            for (int i = 0; i < 50; i++) {
              scores[j][i] = ";;";
            }
            continue;
          }
          
          ScoreDoc[] hits = sm.search(query.substring(0, query.lastIndexOf("OR")).trim());
          
          
          HashMap<Integer, ArrayList<String>> judges = relDocs.getJudges();
          HashMap<String, Integer> judgesList = new HashMap<String, Integer>();
          for(int i = 5; i > 0; i--) {
            ArrayList<String> rdl = judges.get(i);
            if(rdl != null) {
              for(String r: rdl) {
                judgesList.put(r, i);
              }
            }
          }
          
          for (int i = 0; i < hits.length; i++) {
            Document doc = sm.getDocument(hits[i].doc);
            String docId = doc.get("id");
            String score = String.valueOf(hits[i].score);
                                  
            Integer relValue = judgesList.get(docId);
            
            String r = new String("");
            if(relDocsList.contains(docId)) {
              r = new String(docId + " (REL:" + relValue + ");" + score + ";"); 
            } else {
              r = new String(docId + ";" + score + ";");
            }
            scores[j][i] = r;
            if(i == 49) {
              break;
            }
          }
                  
        }
        
        FileManager fq = new FileManager(this.outPath + idQuery + ".csv", Mode.WRITE);
        String head = "Textual;;;Predicate;;;Role;;;Synset;;;Type;;;Uri;;;Concept";
        fq.write(head);
        for(int i = 0; i < 50; i++) {
          StringBuffer row = new StringBuffer("");
          if(scores[0][i] != null) {row.append(scores[0][i] + ";");} else {row.append(";;;");}
          if(scores[1][i] != null) {row.append(scores[1][i] + ";");} else {row.append(";;;");}
          if(scores[2][i] != null) {row.append(scores[2][i] + ";");} else {row.append(";;;");}
          if(scores[3][i] != null) {row.append(scores[3][i] + ";");} else {row.append(";;;");}
          if(scores[4][i] != null) {row.append(scores[4][i] + ";");} else {row.append(";;;");}
          if(scores[5][i] != null) {row.append(scores[5][i] + ";");} else {row.append(";;;");}
          if(scores[6][i] != null) {row.append(scores[6][i] + ";");} else {row.append(";;;");}
          fq.write(row.toString() + ";");
        }
        fq.close();  
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
