package eu.fbk.shell.pikesir.indexer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ar.ArabicAnalyzer;
import org.apache.lucene.analysis.bg.BulgarianAnalyzer;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.cz.CzechAnalyzer;
import org.apache.lucene.analysis.da.DanishAnalyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.el.GreekAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.analysis.fi.FinnishAnalyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.hi.HindiAnalyzer;
import org.apache.lucene.analysis.hu.HungarianAnalyzer;
import org.apache.lucene.analysis.hy.ArmenianAnalyzer;
import org.apache.lucene.analysis.id.IndonesianAnalyzer;
import org.apache.lucene.analysis.it.ItalianAnalyzer;
import org.apache.lucene.analysis.lv.LatvianAnalyzer;
import org.apache.lucene.analysis.nl.DutchAnalyzer;
import org.apache.lucene.analysis.no.NorwegianAnalyzer;
import org.apache.lucene.analysis.pt.PortugueseAnalyzer;
import org.apache.lucene.analysis.ro.RomanianAnalyzer;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.sv.SwedishAnalyzer;
import org.apache.lucene.analysis.th.ThaiAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tr.TurkishAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import eu.fbk.shell.pikesir.datastructure.SemanticStructure;



public class IndexCreator {
  
  private Properties prp;
  private String indexFolder;
  private IndexManager im;
  
  public IndexCreator(Properties p) {
    this.prp = p;
    this.im = new IndexManager(this.prp);
  }
  
  
  public void createIndex(HashMap<String, SemanticStructure> m) {  
    try {
      this.im.createIndex("pikesir", IndexCreator.createAnalyzer());
      Iterator<String> itDocs = m.keySet().iterator();
      
      while(itDocs.hasNext()) {
        
        String docId = itDocs.next();
        SemanticStructure d = m.get(docId);
        
        Document doc = new Document();
        doc.add(new TextField("id", docId, Store.YES));

        ArrayList<String> t = d.getTerms();
        for(String s: t) {
          String[] tokens = s.split("\t");
         
          int frequency = Integer.valueOf(tokens[2]);
          for(int f = 0; f < frequency; f++) {
            if(s.indexOf("inherited=true") == -1 &&
               s.indexOf("len=") == -1 &&
               s.indexOf("certain=false") == -1) {
              doc.add(new TextField(tokens[0], tokens[1].toLowerCase(), Store.YES));
            }          
            doc.add(new TextField(tokens[0] + ".all", tokens[1].toLowerCase(), Store.YES));
             
            if(tokens[0].startsWith("stem") == false && tokens[0].startsWith("lemma") == false) {
              break;
            }
          }
        }
        this.im.addDocument(doc);
      }
      this.im.closeIndex();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  
  
  /*
   * Creates the analyzer object for text analysis
   */
  public static PerFieldAnalyzerWrapper createAnalyzer() {
    Map<String, Analyzer> fieldToAnalyzer = new HashMap<String, Analyzer>();
      
    fieldToAnalyzer.put("raw", new SimpleAnalyzer());
    fieldToAnalyzer.put("lemma.related", new SimpleAnalyzer());
    fieldToAnalyzer.put("lemma.synonym", new SimpleAnalyzer());
    fieldToAnalyzer.put("lemma.text", new SimpleAnalyzer());
    fieldToAnalyzer.put("stem.related", new SimpleAnalyzer());
    fieldToAnalyzer.put("stem.synonym", new SimpleAnalyzer());
    fieldToAnalyzer.put("stem.text", new SimpleAnalyzer());
    fieldToAnalyzer.put("stem.subword", new SimpleAnalyzer());
    fieldToAnalyzer.put("predicate.frb", new KeywordAnalyzer());
    fieldToAnalyzer.put("predicate.nb", new KeywordAnalyzer());
    fieldToAnalyzer.put("predicate.pb", new KeywordAnalyzer());
    fieldToAnalyzer.put("predicate.all", new KeywordAnalyzer());
    fieldToAnalyzer.put("role.frb", new KeywordAnalyzer());
    fieldToAnalyzer.put("role.nb", new KeywordAnalyzer());
    fieldToAnalyzer.put("role.pb", new KeywordAnalyzer());
    fieldToAnalyzer.put("role.all", new KeywordAnalyzer());
    fieldToAnalyzer.put("synset.hypernym", new KeywordAnalyzer());
    fieldToAnalyzer.put("synset.hypernym.all", new KeywordAnalyzer());
    fieldToAnalyzer.put("synset.related", new KeywordAnalyzer());
    fieldToAnalyzer.put("synset.specific", new KeywordAnalyzer());
    fieldToAnalyzer.put("synset.specific.all", new KeywordAnalyzer());
    fieldToAnalyzer.put("type.yago", new KeywordAnalyzer());
    fieldToAnalyzer.put("type.yago.all", new KeywordAnalyzer());
    fieldToAnalyzer.put("type.sumo", new KeywordAnalyzer());
    fieldToAnalyzer.put("type.sumo.all", new KeywordAnalyzer());
    fieldToAnalyzer.put("uri.custom", new KeywordAnalyzer());
    fieldToAnalyzer.put("uri.custom.all", new KeywordAnalyzer());
    fieldToAnalyzer.put("uri.dbpedia", new KeywordAnalyzer());
    fieldToAnalyzer.put("uri.dbpedia.all", new KeywordAnalyzer());
    fieldToAnalyzer.put("concept", new KeywordAnalyzer());
    fieldToAnalyzer.put("concept.all", new KeywordAnalyzer());
        
    PerFieldAnalyzerWrapper analyzer = new PerFieldAnalyzerWrapper(new SimpleAnalyzer(), fieldToAnalyzer);
    return analyzer;
  }
}
