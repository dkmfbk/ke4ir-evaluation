package com.dragotech.tools;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
//import java.util.HashMap;

public class DirectoryManager {
  private String absolutePath;
  private int docCount = 0;
  private int currentDocIndex = 0;
  private ArrayList docList;

  public DirectoryManager(String absolutePath) {
    this.absolutePath = absolutePath;
    this.docList = new ArrayList<String>();
    // this.docFlags = new HashMap<Integer, Integer>();
  }

  /**
   * Get the list of files present at the root level
   */
  public void createDocumentList() {
    File curDir = new File(this.absolutePath);
    String[] curFiles = curDir.list();
    Arrays.sort(curFiles);
    this.elaborateFolder(curFiles, this.absolutePath);
    System.out.println("Document list loaded: " + this.docCount + " elements.");
  }

  /**
   * Support to the elaborateDocuments function to loop over all files
   */
  private void elaborateFolder(String[] files, String curPath) {
    for (int i = 0; i < files.length; i++) {
      if (files[i].indexOf(".svn") == 0) {
        continue;
      }
      File curFile = new File(curPath + files[i]);
      if (curFile.isDirectory()) {
        String[] folderFiles = curFile.list();
        Arrays.sort(folderFiles);
        /*
         * String CurFolder = CurPath + Files[I] + "\\"; CurFolder = CurFolder.replace(MainInputPath, MainOutputPath);
         * File OD = new File(CurFolder); boolean A = OD.mkdir();
         */
        this.elaborateFolder(folderFiles, curPath + files[i] + "/");
      } else {
        // this.docList.put(new Integer(docCount), curFile.getAbsolutePath());
        this.docList.add(curFile.getAbsolutePath());
        // this.docFlags.put(new Integer(docCount), new Integer(0));
        this.docCount++;
      }
    }
  }

  /**
   * Get the next document to elaborate
   */
  /*
   * synchronized public String[] getNextDoc() { String[] nextDoc = new String[2]; String nextDocName =
   * this.docList.get(new Integer(this.currentDocIndex)); if(nextDocName == null) return null; nextDoc[0] = nextDocName;
   * //NextDoc[1] = NextDoc[0].replace(MainInputPath, MainOutputPath); this.currentDocIndex++; //this.docFlags.put(new
   * Integer(docCount), new Integer(1)); //if(this.CurrentDocIndex % 1000 == 0)
   * System.out.println(this.CurrentDocIndex); return nextDoc; }
   */

  public synchronized String getNextDoc() {
    
    if(this.currentDocIndex == this.docList.size()) return null;
    String nextDocName = (String) this.docList.get(new Integer(this.currentDocIndex));
    if (nextDocName == null) {
      return null;
    }
    this.currentDocIndex++;
    return nextDocName;
  }

  /**
   * Get the next document to elaborate
   */
  public String setDocFlag(int countID) {
    // this.docFlags.put(new Integer(docCount), new Integer(1));
    return null;
  }

  /**
   * Debug methods to explore the produced HashMaps
   */

  public ArrayList<String> getDocList() {
    return this.docList;
  }
  // public HashMap<Integer, Integer> getDocFlags(){return this.docFlags;}
}
