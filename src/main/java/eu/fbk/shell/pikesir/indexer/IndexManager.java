package eu.fbk.shell.pikesir.indexer;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;



public class IndexManager {

  private Properties prp;
  private String indexFolder;
  private Directory indexDir;
  private IndexWriterConfig config;
  private IndexWriter indexWriter;
  
  public IndexManager(Properties p) {
    this.prp = p;
    this.indexFolder = this.prp.getProperty("pikesir.indexfolder");
  }
  
  
  
  /**
   * Create a new index object.
   * @param indexId
   */
  public void createIndex(String indexId, PerFieldAnalyzerWrapper analyzer) {
    try {
      this.indexDir = FSDirectory.open(new File(this.indexFolder + indexId));
      this.config = new IndexWriterConfig(Version.LUCENE_4_10_3, analyzer);
      this.indexWriter = new IndexWriter(this.indexDir, this.config);      
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  
  
  /**
   * Add a new document to the index.
   * @param doc
   */
  public void addDocument(Document doc) {
    try {
      this.indexWriter.addDocument(doc);
      this.indexWriter.commit();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  
  
  /**
   * Close the index.
   */
  public void closeIndex() {
    try {
      this.indexWriter.close();
    } catch (IOException e) {
      e.printStackTrace();
    } 
  }
  
}
