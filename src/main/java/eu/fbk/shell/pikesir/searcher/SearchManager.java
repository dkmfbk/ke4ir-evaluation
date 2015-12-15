package eu.fbk.shell.pikesir.searcher;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;



public class SearchManager {

  private Properties prp;
  private PerFieldAnalyzerWrapper analyzer;
  //private StandardAnalyzer analyzer;
  private String indexesFolder;
  private String indexId;
  private IndexSearcher searcher;
  
  public SearchManager(Properties p) {
    this.prp = p;
    this.indexesFolder = this.prp.getProperty("pikesir.indexfolder");
  }
  
  
  public void openIndex(String indexId, PerFieldAnalyzerWrapper analyzer) {
    try {
      this.indexId = indexId;
      this.analyzer = analyzer;
      this.searcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(new File(this.indexesFolder + this.indexId).toPath())));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  
  public Document getDocument(int docId) {
    try {
      return this.searcher.doc(docId);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }
  
  
  public ScoreDoc[] search(String query) {
    try {
      //System.out.println(indexId + " --- " + query);
      QueryParser parser = new QueryParser("complete-text", this.analyzer);
      Query q = null;
      try {
        //parser.setPhraseSlop(5);
        q = parser.parse(query);
        TopDocs rank = this.searcher.search(q, 1000);
        ScoreDoc[] hits = rank.scoreDocs;
        return hits;
      } catch (Exception e) {
        System.out.println(query);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }
  
  

  /*
  public String[] findFirstMap(String query) {
    String[] matchedEntity = new String[2];
    matchedEntity[0] = null;
    matchedEntity[1] = new String("-1.0");
    try {
      System.out.println(indexId + " --- " + query);
      QueryParser parser = new QueryParser("label-text", this.analyzer);
      Query q = null;
      try {
        parser.setPhraseSlop(5);
        q = parser.parse(query);
        TopDocs rank = this.searcher.search(q, 1);
        ScoreDoc[] hits = rank.scoreDocs;
        for (int i = 0; i < hits.length; i++) {
          //System.out.println(hits[i].score);
          if(hits[i].score < 3.0) continue;
          Document doc = this.searcher.doc(hits[i].doc);
          matchedEntity[0] = doc.get("id");
          matchedEntity[1] = String.valueOf(hits[i].score);
        }
      } catch (Exception e) {
        //System.out.println(query[0]);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return matchedEntity;
  }
  */
  
}
