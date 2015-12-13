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


public class Searcher {
  
  private Properties prp;
  private String outPath;
  private String mode;
  
  public Searcher(Properties p) {
    this.prp = p;
    this.outPath = prp.getProperty("pikesir.output.aggregates");
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
       * Dalla riga 145 (circa) in poi, ci sono i controlli sull'appartenenza di un campo ad un layer.
       */
      //String[] layerLemma = {"lemma.related", "lemma.synonym", "lemma.text"};
      //String[] layerStem = {"stem.related", "stem.synonym", "stem.text"};
      //String[] layerTextual = {"lemma.related", "lemma.synonym", "lemma.text", "stem.related", "stem.synonym", "stem.text"};
      //String[] layerTextual = {"lemma.text", "stem.text"};
      //String[] layerTextual = {"lemma.synonym", "lemma.text", "stem.subword", "stem.synonym", "stem.text"};
      //String[] layerTextual = {"lemma.text", "stem.text"};
      String[] layerTextual = {"stem.subword"};
      
      String[] layerPredicate = {"predicate.frb", "predicate.nb", "predicate.pb"};
      //String[] layerPredicate = {"predicate.frb"};
      
      String[] layerRole = {"role.frb", "role.nb", "role.pb"};
      //String[] layerRole = {"role.frb"};
      
      String[] layerSynset = {"synset.hypernym", "synset.hypernym.amb", "synset.related", "synset.specific", "synset.specific.amb"};
      //String[] layerSynset = {"synset.related", "synset.specific", "synset.specific.amb"};
      
      String[] layerType = {"type.yago", "type.sumo"};
//      String[] layerType = {"type.yago"};
      
      String[] layerURI = {"uri.custom", "uri.dbpedia"};
//      String[] layerURI = {"uri.dbpedia"};
      
      String[] layerConcept = {"concept"};
      
      
      /*
      String[] layerTextual = {"lemma.synonym"};      
      String[] layerPredicate = {"lemma.text"};
      String[] layerRole = {"lemma.related"};
      String[] layerSynset = {"stem.text"};
      String[] layerType = {"stem.synonym"};
      String[] layerURI = {"stem.related"};
      String[] layerConcept = {"stem.subword"};
      */
      
      
      /*
       * Lista delle etichette che compaiono nella prima colonna dell'excel per descrivere i layers utilizzati per ciascuna combinazione.
       * L'ordine delle etichette e' lo stesso dei layer elencati qui sopra.
       */
      //String[] fieldsSet = {"lemma", "stem", "predicate", "role", "synset", "type", "uri", "concept"};
      String[] fieldsSet = {"textual", "predicate", "role", "synset", "type", "uri", "concept"};
      //String[] fieldsSet = {"lemma.synonym", "lemma.text", "lemma.related", "stem.text", "stem.synonym", "stem.related", "stem.subword"};
      
      /*
       * Configurazione iniziale.
       * Se aggiungi un layer aggiungi uno '0' alla fine ed alla fine del blocco di estrazione campi qui sotto, fare riferimento
       * all'indice dell'array corretto (anche in caso di rimozione di un layer).
       */
      int[] fieldsFlag = {1, 0, 0, 0, 0, 0, 0};
      
      
      
      HashMap<String, Double[]> results = new HashMap<String, Double[]>();
      HashMap<String, Integer> rescale = new HashMap<String, Integer>();
      
      int execFlag = 1;
      while(execFlag == 1) {
       execFlag = 0;
        String layerList = new String("");
        for(int j = 0; j < fieldsFlag.length; j++) {
          if(fieldsFlag[j] == 1) {
            layerList = layerList + fieldsSet[j] + ",";
          }
        }
        layerList = layerList.substring(0, layerList.lastIndexOf(","));
        
        double prec1 = 0.0;
        double rr = 0.0;
        double prec3 = 0.0;
        double prec5 = 0.0;
        double prec10 = 0.0;
        double ndcg = 0.0;
        double ndcg10 = 0.0;
        double map = 0.0;
        double map10 = 0.0;
        int numberOfQueries = queries.keySet().size();
        
        Iterator<String> itQuery = queries.keySet().iterator();
        while(itQuery.hasNext()) {
          String idQuery = itQuery.next();
          SemanticStructure q = queries.get(idQuery);
          QueryJudgments relDocs = relevance.get(idQuery);
          ArrayList<String> relDocsList = relDocs.getRelDocsList();
          
          ArrayList<String> t = q.getTerms();
          StringBuffer query = new StringBuffer("");
          for(String s: t) {
            String[] tokens = s.split("\t");
            
            
            /*
            if(Arrays.asList(layerLemma).contains(tokens[0]) && fieldsFlag[0] == 1) {
              //query.append(tokens[0] + ":\"" + tokens[1].toLowerCase() + "\" OR ");
              query.append("lemma.text:\"" + tokens[1].toLowerCase() + "\" OR ");
            } else if(Arrays.asList(layerStem).contains(tokens[0]) && fieldsFlag[1] == 1) {
              //query.append(tokens[0] + ":\"" + tokens[1].toLowerCase() + "\" OR ");
              query.append("stem.text:\"" + tokens[1].toLowerCase() + "\" OR ");
            */
            if(Arrays.asList(layerTextual).contains(tokens[0]) && fieldsFlag[0] == 1) {
              if(tokens[0].startsWith("lemma")) {
                //query.append("lemma.text:\"" + tokens[1].toLowerCase() + "\" OR ");
              } else if(tokens[0].startsWith("stem")) {
                query.append("stem.subword:\"" + tokens[1].toLowerCase() + "\" OR ");
                query.append("stem.subword:\"" + tokens[1].toLowerCase() + "\" OR ");
                query.append("stem.subword:\"" + tokens[1].toLowerCase() + "\" OR ");
                query.append("stem.subword:\"" + tokens[1].toLowerCase() + "\" OR ");
              }
              
              // From this point, the index value of the array "fieldsFlag" has been decreased by one due to the join of the two
              // arrays "lemma" and "stem"
            } else if(Arrays.asList(layerPredicate).contains(tokens[0]) && fieldsFlag[1] == 1) {
              if(this.mode.compareTo("A") == 0) {
                query.append(tokens[0] + ".all:\"" + tokens[1].toLowerCase() + "\" OR ");
              } else if(this.mode.compareTo("B") == 0) {
                if(s.indexOf("inherited") == -1) {
                  query.append(tokens[0] + ".all:\"" + tokens[1].toLowerCase() + "\" OR ");
                }
              }
            } 
            
            else if(Arrays.asList(layerRole).contains(tokens[0]) && fieldsFlag[2] == 1) {
              if(this.mode.compareTo("A") == 0) {
                query.append(tokens[0] + ".all:\"" + tokens[1].toLowerCase() + "\" OR ");
              } else if(this.mode.compareTo("B") == 0) {
                if(s.indexOf("inherited") == -1) {
                  query.append(tokens[0] + ".all:\"" + tokens[1].toLowerCase() + "\" OR ");
                }
              }
            } 
            
            else if(Arrays.asList(layerSynset).contains(tokens[0]) && fieldsFlag[3] == 1) {
              if(this.mode.compareTo("A") == 0) {
                query.append(tokens[0] + ".all:\"" + tokens[1].toLowerCase() + "\" OR ");
              } else if(this.mode.compareTo("B") == 0) {
                if(s.indexOf("inherited") == -1 && s.indexOf("certain=false") == -1 && s.indexOf("len") == -1) {
                  query.append(tokens[0] + ".all:\"" + tokens[1].toLowerCase() + "\" OR ");
                }
              }
            } 
            
            else if(Arrays.asList(layerType).contains(tokens[0]) && fieldsFlag[4] == 1) {
              if(this.mode.compareTo("A") == 0) {
                query.append(tokens[0] + ".all:\"" + tokens[1].toLowerCase() + "\" OR ");
              } else if(this.mode.compareTo("B") == 0) {
                if(s.indexOf("inherited") == -1) {
                  query.append(tokens[0] + ".all:\"" + tokens[1].toLowerCase() + "\" OR ");
                }
              }
            } 
            
            else if(Arrays.asList(layerURI).contains(tokens[0]) && fieldsFlag[5] == 1) {
              if(this.mode.compareTo("A") == 0) {
                query.append(tokens[0] + ".all:\"" + tokens[1].toLowerCase() + "\" OR ");
              } else if(this.mode.compareTo("B") == 0) {
                if(s.indexOf("inherited") == -1) {
                  query.append(tokens[0] + ".all:\"" + tokens[1].toLowerCase() + "\" OR ");
                }
              }
            } 
            
            else if(Arrays.asList(layerConcept).contains(tokens[0]) && fieldsFlag[6] == 1) {
              if(this.mode.compareTo("A") == 0) {
                query.append(tokens[0] + ".all:\"" + tokens[1].toLowerCase() + "\" OR ");
              } else if(this.mode.compareTo("B") == 0) {
                if(s.indexOf("inherited") == -1) {
                  query.append(tokens[0] + ".all:\"" + tokens[1].toLowerCase() + "\" OR ");
                }
              }
            }
          }
          
          if(query.length() == 0) {
            continue;
          }
          ScoreDoc[] hits = sm.search(query.substring(0, query.lastIndexOf("OR")).trim());
          Integer cScale = rescale.get(layerList);
          if(cScale == null) {
            cScale = 0;
          }
          cScale++;
          rescale.put(layerList, cScale);
          
          
          /* Computing the ideal discounted cumulated gain */
          HashMap<Integer, ArrayList<String>> judges = relDocs.getJudges();
          HashMap<String, Integer> judgesList = new HashMap<String, Integer>();
          int counter = 1;
          double dcg = 0.0;
          double dcg10 = 0.0;
          double indcg = 0.0;
          double indcg10 = 0.0;
          for(int i = 5; i > 0; i--) {
            ArrayList<String> rdl = judges.get(i);
            if(rdl != null) {
              for(String r: rdl) {
                judgesList.put(r, i);
                if(counter == 1) {
                  indcg += (double) i;
                  indcg10 += (double) i;
                } else {
                  indcg += (double) i / (Math.log(counter) / Math.log(2));
                  if(counter < 11) {
                    indcg10 += (double) i / (Math.log(counter) / Math.log(2));
                  }
                }
                counter++;
              }
            }
          }
          
          System.out.print("\n"+ idQuery);
          
          double localMap = 0.0;
          double localMap10 = 0.0;
          double localPrec1 = 0.0;
          double localPrec3 = 0.0;
          double localPrec5 = 0.0;
          double localPrec10 = 0.0;
          double localRR = 0.0;
          int relDocFound = 0;
          int numRelDoc = judgesList.keySet().size();
          for (int i = 0; i < hits.length; i++) {
            Document doc = sm.getDocument(hits[i].doc);
            String docId = doc.get("id");
            String score = String.valueOf(hits[i].score);
            
            System.out.print("\t" + docId);
            
            Integer relValue = judgesList.get(docId);
            if(relValue != null) {
              relDocFound++;
              
              if(relDocFound == 1) {
                localRR += 1.0 / (double) (i + 1);
              }
              
              localMap += (double) relDocFound / (double) (i + 1);
              if(i < 10) {
                localMap10 += (double) relDocFound / (double) (i + 1);
              }
              
              if(i == 0) {
                dcg += (double) relValue;
                dcg10 += (double) relValue;
              } else {
                dcg += (double) relValue / (Math.log(i + 1) / Math.log(2));
                if(i < 10) {
                  dcg10 += (double) relValue / (Math.log(i + 1) / Math.log(2));
                }
              }
            }
            
            if(i == 0) {
              if(relDocsList.contains(docId)) {
                localPrec1 += 1.0;
                localPrec3 += 1.0;
                localPrec5 += 1.0;
                localPrec10 += 1.0;
              }
            } else if(i < 3) {
              if(relDocsList.contains(docId)) {
                localPrec3 += 1.0;
                localPrec5 += 1.0;
                localPrec10 += 1.0;
              }
            } else if(i < 5) {
              if(relDocsList.contains(docId)) {
                localPrec5 += 1.0;
                localPrec10 += 1.0;
              }
            } else if(i < 10) {
              if(relDocsList.contains(docId)) {
                localPrec10 += 1.0;
              }
            } else {
              if(relDocsList.contains(docId)) {
              }
            }
          }
          map += localMap / (double) numRelDoc;
          map10 += localMap10 / (double) numRelDoc;
          ndcg += dcg / indcg;
          ndcg10 += dcg10 / indcg10;
          prec1 += localPrec1;
          prec3 += localPrec3;
          prec5 += localPrec5;
          prec10 += localPrec10;
          rr += localRR;
          //System.out.println();
          //System.out.println((dcg / indcg) + " --- " + (dcg10 / indcg10) + " --- " + (localMap / (double) numRelDoc) + 
          //                   " --- " + (localMap10 / (double) numRelDoc));
          
          FileManager fq = new FileManager(this.outPath + idQuery + ".csv", Mode.APPEND);
          String row = new String(layerList + ";" + localPrec1 + ";" + (localPrec3 / 3.0) + ";" + (localPrec5 / 5.0) + ";" + 
                                  (localPrec10 / 10.0) + ";" + localRR + ";" + (dcg / indcg) + ";" + (dcg10 / indcg10) +
                                  ";" + (localMap / (double) numRelDoc) + ";" + (localMap10 / (double) numRelDoc));
          fq.write(row);
          fq.close();
          
          System.err.println(idQuery + " -> " + (dcg / indcg));
          
        }
        //System.out.println();
        /*
        System.out.println("Precision@1: " + (prec1 / (double) numberOfQueries));
        System.out.println("Precision@3: " + (prec3 / (double) (numberOfQueries * 3)));
        System.out.println("Precision@5: " + (prec5 / (double) (numberOfQueries * 5)));
        System.out.println("Precision@10: " + (prec10 / (double) (numberOfQueries * 10)));
        System.out.println("Reciprocal Rank: " + (rr / (double) numberOfQueries));
        System.out.println("nDCG: " + (ndcg / (double) numberOfQueries));
        System.out.println("nDCG@10: " + (ndcg10 / (double) numberOfQueries));
        System.out.println("MAP: " + (map / (double) numberOfQueries));
        System.out.println("MAP@10: " + (map10 / (double) numberOfQueries));
        */
        
        Integer rescaleFactor = rescale.get(layerList);
        Double[] runResults = {
            (prec1 / (double) rescaleFactor),
            (prec3 / (double) (rescaleFactor * 3)),
            (prec5 / (double) (rescaleFactor * 5)),
            (prec10 / (double) (rescaleFactor * 10)),
            (rr / (double) rescaleFactor),
            (ndcg / (double) rescaleFactor),
            (ndcg10 / (double) rescaleFactor),
            (map / (double) rescaleFactor),
            (map10 / (double) rescaleFactor)
        };
        results.put(layerList, runResults);
        
        
        /* Update the fieldFlag list for switching to the next run */
        fieldsFlag[0] += 1;
        for(int j = 0; j < fieldsFlag.length - 1; j++) {
          if(fieldsFlag[j] == 2) {
            fieldsFlag[j+1]++;
            fieldsFlag[j] = 0;
          }
          System.out.print(fieldsFlag[j]);
        }
        System.out.print(fieldsFlag[fieldsFlag.length - 1]);
        if(fieldsFlag[fieldsFlag.length - 1] == 2) {
          break;
        }
        System.out.println();
      }
      System.out.println();
      System.out.println();
      
      
      
      FileManager fq = new FileManager(this.outPath + "aggregates.csv", Mode.WRITE);
      Iterator<String> itLayers = results.keySet().iterator();
      while(itLayers.hasNext()) {
        String l = itLayers.next();
        StringBuffer row = new StringBuffer(l + ";"); 
        Double[] r = results.get(l);
        for(int j = 0; j < r.length; j++) {
          row.append(r[j] + ";");
        }
        fq.write(row.toString());
      }
      fq.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
