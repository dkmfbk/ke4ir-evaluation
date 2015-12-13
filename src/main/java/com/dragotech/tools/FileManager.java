package com.dragotech.tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;

public class FileManager {

  private String fileName;
  private File currentFile;
  private String absolutePath;
  private PrintWriter outputStream;
  private BufferedReader inputStream;
  private FileReader inputReader;
  private ArrayList rowsList;

  /**
   * 0: input 1: output
   */
  public enum Mode {
    READ, WRITE, APPEND
  };

  private Mode mode;

  /**
   * Initializes the object
   * 
   * @param FileAbsolutePath
   */
  public FileManager(String fileAbsolutePath, Mode m) {

    this.absolutePath = fileAbsolutePath;
    this.currentFile = new File(fileAbsolutePath);
    this.fileName = this.currentFile.getName();
    this.mode = m;

    try {
      if (this.mode == Mode.READ) {
        this.inputStream = new BufferedReader(new FileReader(fileAbsolutePath));
        this.inputReader = new FileReader(fileAbsolutePath);
      } else if (this.mode == Mode.WRITE) {
        this.outputStream = new PrintWriter(new BufferedWriter(new FileWriter(fileAbsolutePath)));
      } else if (this.mode == Mode.APPEND) {
        this.outputStream = new PrintWriter(new BufferedWriter(new FileWriter(fileAbsolutePath, true)));
      }

    } catch (IOException e) {
      e.printStackTrace();
      return;
    }
  }

  /**
   * Writes a string in the file.
   * 
   * @param DataStream
   */
  public void write(String dataStream) {
    if (this.mode == Mode.WRITE || this.mode == Mode.APPEND) {
      this.outputStream.println(dataStream);
    }
  }

  /**
   * Reads a line from the file
   * 
   * @return
   * @throws IOException
   */
  public String read() {
    try {
      if (this.mode == Mode.READ) {
        String dataStream;
        dataStream = this.inputStream.readLine();
        return dataStream;
      }
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
    return null;
  }

  /**
   * Closes the file
   */
  public void close() {
    try {
      if (this.mode == Mode.READ) {
        this.inputStream.close(); 
      } else if (this.mode == Mode.WRITE || this.mode == Mode.APPEND) {
        this.outputStream.flush();
        this.outputStream.close();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  /**
   * Methods for managing different kinds of file
   */
  public ArrayList<String> importSimpleTextContent() {
    this.rowsList = new ArrayList<String>();
    String rowString = this.read();
    while (rowString != null) {
      this.rowsList.add(rowString);
      rowString = this.read();
    }
    return this.rowsList;
  }

  public String importFullTextContent() {
    
    StringBuffer content = new StringBuffer();
    String dataStream = this.read();
    while (dataStream != null) {
      content = content.append(dataStream + "\n");
      dataStream = this.read();
    }
    return content.toString();
    
    /*
    try {
      StringBuffer fileData = new StringBuffer();
      BufferedReader reader = new BufferedReader(this.inputReader);
      char[] buf = new char[1024];
      int numRead = 0;
      while((numRead = reader.read(buf)) != -1) {
        String readData = String.valueOf(buf, 0, numRead);
        fileData.append(readData);
        buf = new char[1024];
      }
      reader.close();
      return fileData.toString();
    } catch(Exception e) {
      e.printStackTrace();
    }
    return null;*/
  }

  public void importTabSeparatedContent() {

  }

  
  public ArrayList<String[]> importStringSeparatedContent(String splitter) {
    
    ArrayList<String[]> splittedData = new ArrayList<String[]>();
    String dataStream = this.read();
    while (dataStream != null) {
      String[] content = dataStream.split(splitter);
      splittedData.add(content);
      dataStream = this.read();
    }
    
    return splittedData;
  }

  public Document importXMLContent() {
    Document doc = null;
    try {
      DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
      DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
      doc = dBuilder.parse(this.currentFile);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return doc;
  }

}
