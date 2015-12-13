package com.dragotech.tools;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Vector;

public class ServiceLayer
{
	private ServiceLayer(){}
	
	static public String[] getAirportCodes()
	{
		Vector<String> V = new Vector<String>();
		try
		{
			String Query = "SELECT codicemetar FROM localita_task_387 WHERE flagselected = 1 " +
										 "ORDER BY codicemetar ASC";
			ResultSet R = DBLayer.SQL(Query);
			while(R.next())
			{
				String S = new String(R.getString(1));
				V.add(S);
			}
			
			String[] Codes = new String[V.size()];
			for(int I = 0; I < V.size(); I++)
			{
				Codes[I] = (String)V.elementAt(I);
			}
			return Codes;
		}
		catch(SQLException e)
		{
			e.printStackTrace();
			return null;
		}
	}
	
	
	
	static public double[][] getLocalita() {

	  double[][] results = new double[500][5];
	  try {
	    	  
  	  String Query = "SELECT * FROM localita WHERE flag_update = 0 ORDER BY idloc ASC LIMIT 500";
  	  //Query = "SELECT * FROM localita WHERE provcap = 1 ORDER BY idloc ASC LIMIT 500";
      ResultSet R = DBLayer.SQL(Query);
      int i = 0;
      while(R.next())
      {
        double idloc = (double) R.getInt("idloc");
        double idreg = (double) R.getInt("idreg");
        double idprov = (double) R.getInt("idprov");
        double lat = R.getDouble("lat");
        double lon = R.getDouble("lon");
        results[i][0] = idloc;
        results[i][1] = idreg;
        results[i][2] = idprov;
        results[i][3] = lat;
        results[i][4] = lon;
        i++;
      }
      
      if(i == 0) {
        Query = "UPDATE localita SET flag_update = 0 WHERE idloc > 0";
        R = DBLayer.SQL(Query);
        Query = "SELECT * FROM localita WHERE flag_update = 0 ORDER BY idloc ASC LIMIT 500";
        R = DBLayer.SQL(Query);
        i = 0;
        while(R.next())
        {
          double idloc = (double) R.getInt("idloc");
          double idreg = (double) R.getInt("idreg");
          double idprov = (double) R.getInt("idprov");
          double lat = R.getDouble("lat");
          double lon = R.getDouble("lon");
          results[i][0] = idloc;
          results[i][1] = idreg;
          results[i][2] = idprov;
          results[i][3] = lat;
          results[i][4] = lon;
          i++;
        }
      }
	  } catch(Exception e) {
	    e.printStackTrace();
	  }
	  return results;
	}
	
	
	
	static public String getRegioneByNomeProvincia(String nomeProvincia) {
	  String regione = "";
	  try {  
      String Query = "SELECT r.nome as nomereg FROM regioni r LEFT JOIN province p ON r.idreg = p.idreg WHERE p.nome = \"" + 
                      nomeProvincia + "\"";
      ResultSet R = DBLayer.SQL(Query);
      int i = 0;
      while(R.next())
      {
        regione = R.getString("nomereg");
      }
	  } catch(Exception e) {
      e.printStackTrace();
    }
	  return regione;
	}
	
	
	
	@SuppressWarnings("unused")
  static public void executeUpdateQuery(String Query)
	{
		ResultSet R = DBLayer.SQL(Query);
	}
}
