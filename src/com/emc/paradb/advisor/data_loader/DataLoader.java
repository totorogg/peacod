package com.emc.paradb.advisor.data_loader;

import java.sql.Connection;
import java.sql.DriverManager;


public class DataLoader
{
	private String selectedDB = null;
	private String selectedBM = null;
	
	public DataLoader(String selectedDB, String selectedBM)
	{	
		this.selectedBM = selectedBM;
		this.selectedDB = selectedDB;
	}
	
	public void load() throws Exception
	{
		if(selectedDB.equals("PostgreSQL"))
		{
			PostgreSQLLoader pgLoader = new PostgreSQLLoader(selectedBM);
			pgLoader.loadData();
		}
		else if(selectedDB.equals("MySQL"))
		{
			throw new Exception("No support for MySQL yet");
		}
	}
}


