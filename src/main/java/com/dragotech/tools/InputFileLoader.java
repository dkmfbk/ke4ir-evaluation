/*
 *    Copyright (c) Sematext International
 *    All Rights Reserved
 *
 *    THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF Sematext International
 *    The copyright notice above does not evidence any
 *    actual or intended publication of such source code.
 */
package com.dragotech.tools;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

public class InputFileLoader {

  private Properties prp;
  private ArrayList<String> metarCodes;
  private HashMap<String, String> wuSkyStatusMap;
  private HashMap<String, String> associationRegionZoneItaly;
  private HashMap<String, Integer> lightValues;
  private HashMap<String, String> associationMetarLocation;
  private HashMap<String, String> associationProvinceMetar;
  
  /* Key: stationId => value: regione;provincia;nome */
  private HashMap<String, String> stationToOutputString;
  
  public InputFileLoader(Properties p) {
    this.prp = p;
    this.metarCodes = new ArrayList<String>();
    this.wuSkyStatusMap = new HashMap<String, String>();
    this.associationRegionZoneItaly = new HashMap<String, String>();
    this.associationMetarLocation = new HashMap<String, String>();
    this.associationProvinceMetar = new HashMap<String, String>();
    this.stationToOutputString = new HashMap<String, String>();
    this.lightValues = new HashMap<String, Integer>();
  }
  
  
  /**
   * Loading METAR stations data
   */
  public void loadMetarFile() {
    try {
      //BufferedReader metarFile = new BufferedReader(new FileReader(this.prp.getProperty("metarcodes.filename"))); 
      BufferedReader metarFile = new BufferedReader(new InputStreamReader(new FileInputStream(this.prp.getProperty("metarcodes.filename")), "UTF-8"));
      
      String dataStream = metarFile.readLine();
      while(dataStream != null) {
        //System.out.println(dataStream);
        String[] currentMetarCode = dataStream.split(";", 2);
        this.metarCodes.add(currentMetarCode[0]);
        if(currentMetarCode.length > 3) {
          this.associationRegionZoneItaly.put(currentMetarCode[0], currentMetarCode[3].trim());
        }
        this.associationMetarLocation.put(currentMetarCode[0], currentMetarCode[1].trim());
        this.associationProvinceMetar.put(currentMetarCode[1], currentMetarCode[0].trim());
        this.stationToOutputString.put(currentMetarCode[0].trim(), currentMetarCode[1].trim());
        dataStream = metarFile.readLine();        
      }
      System.out.println(this.metarCodes.size() + " Metar codes station loaded.");
      //System.exit(0);
    } catch (FileNotFoundException e1) {
      System.out.println("Impossibile aprire il file.");
    } catch (IOException e) {
      System.out.println("Impossibile leggere il file.");
    }
  }
  
  public ArrayList<String> getMetarCodesToAnalyze() {
    return this.metarCodes;
  }
  
  public HashMap<String, String> getAssociationRegioneZoneItalyMap() {
    return this.associationRegionZoneItaly;
  }
  
  public HashMap<String, String> getAssociationMetarLocationMap() {
    return this.associationMetarLocation;
  }
    
  public HashMap<String, String> getAssociationProvinceMetarMap() {
    return this.associationProvinceMetar;
  }
  
  public HashMap<String, String> getStationToOutputStringMap() {
    return this.stationToOutputString;
  }
  
  
  
  /**
   * Loading WU analysis information
   */
  public void loadWUAnalyzerFiles() {
    try {
      BufferedReader wuSkyStatusFile = new BufferedReader(new FileReader(this.prp.getProperty("wuskystatusmap.filename"))); 
      BufferedReader lightValuesFile = new BufferedReader(new FileReader(this.prp.getProperty("lightvalues.filename")));
      
      String dataStream = wuSkyStatusFile.readLine();
      while(dataStream != null) {
        String[] currentStatus = dataStream.split(",");
        this.wuSkyStatusMap.put(currentStatus[1], currentStatus[0]);
        dataStream = wuSkyStatusFile.readLine();        
      }
      System.out.println(this.wuSkyStatusMap.size() + " WU sky status map loaded.");
      
      
     dataStream = lightValuesFile.readLine();
      while(dataStream != null) {
        String[] currentStatus = dataStream.split(",");
        this.lightValues.put(currentStatus[0], new Integer(Integer.parseInt(currentStatus[1])));
        dataStream = lightValuesFile.readLine();        
      }
      System.out.println(this.lightValues.size() + " light values loaded.");
      
    } catch (FileNotFoundException e1) {
      System.out.println("Impossibile aprire il file.");
    } catch (IOException e) {
      System.out.println("Impossibile leggere il file.");
    }
  }
  
  public HashMap<String, String> getWUSkyStatusMap() {
    return this.wuSkyStatusMap;
  }
  
  public HashMap<String, Integer> getLightValuesMap() {
    return this.lightValues;
  }
}
