package eu.fbk.shell.pikesir.datastructure;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class QueryJudgments {
  
  private String id;
  private HashMap<Integer, ArrayList<String>> judges;
  
  public QueryJudgments(String id) {
    this.id = id;
    this.judges = new HashMap<Integer, ArrayList<String>>();
  }
  
  private String getId() {
    return this.id;
  }
  
  public void addJudge(int relevance, String t) {
    ArrayList<String> j = this.judges.get(relevance);
    if(j == null) {
      j = new ArrayList<String>();
    }
    j.add(t);
    this.judges.put(relevance, j);
  }
  
  public HashMap<Integer, ArrayList<String>> getJudges() {
    return this.judges;
  }
  
  public ArrayList<String> getRelDocsList() {
    ArrayList<String> relDocs = new ArrayList<String>();
    for(int i = 5; i > -1; i--) {
      ArrayList<String> d = this.judges.get(i);
      if(d != null) {
        relDocs.addAll(d);
      }
    }
    
    return relDocs;
  }
}
