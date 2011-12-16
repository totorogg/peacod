package com.emc.paradb.advisor.data_loader;

import java.sql.Connection;
import java.sql.DriverManager;



public class PostgreSQLLoader
{
	private String selectedBM = null;
	private String IP = "10.32.216.106";
	private String port = "12345";
	private String user = "postgres";
	private String password = "";
	private String db = null;
	Connection conn = null;
	
	public PostgreSQLLoader(String selectedBM) throws Exception{
		
		this.selectedBM = selectedBM;
		
		if(selectedBM.equalsIgnoreCase("tpc-c"))
		{
			db = "dbt2";
		}
		else
		{
			throw new Exception("other benchmark not implemeneted yet");
		}
		
		getConnection();
	}
	
	public void loadData()
	{
		
		
	}
	
	public Connection getConnection()
	{		
		try
		{
			Class.forName("org.postgresql.Driver").newInstance();
			conn = DriverManager.getConnection(
					String.format("jdbc:postgresql://%s:%s/%s", IP, port, db), user, password);
			
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
			e.getStackTrace();
		}
		return conn;
	}
}