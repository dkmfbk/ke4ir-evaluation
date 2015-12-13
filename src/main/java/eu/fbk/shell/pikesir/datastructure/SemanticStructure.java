package eu.fbk.shell.pikesir.datastructure;

import java.util.ArrayList;

public class SemanticStructure {
  
  private String id;
  private ArrayList<String> terms;
  
  public SemanticStructure(String id) {
    this.id = id;
    this.terms = new ArrayList<String>();
  }
  
  private String getId() {
    return this.id;
  }
  
  public void addTerm(String t) {
    this.terms.add(t);
  }
  
  public ArrayList<String> getTerms() {
    return this.terms;
  }
}
