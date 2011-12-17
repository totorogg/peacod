package com.emc.paradb.advisor.data_loader;

import java.sql.Connection;
import java.sql.DriverManager;



public class PostgreSQLLoader extends DataLoader
{
	private String db = null;
	private Connection conn = null;
	private PGMetaLoader pgMetaLoader = null;
	//PGDataLoader pgDataLoader = null;
	
	public PostgreSQLLoader(String selectedBM) throws Exception{
		super(selectedBM);
		
		if(selectedBM.equalsIgnoreCase("tpc-c"))
			db = "dbt2";
		else
			throw new Exception("other benchmark not implemeneted yet");

		conn = PGConnector.getConnection(db);
	}
	
	public float getProgress()
	{
		return pgMetaLoader.getProgress();
	}
	public void load()
	{
		pgMetaLoader = new PGMetaLoader(conn);
		pgMetaLoader.load();
		//PGDataLoader pgDataLoader = new PGDataLoader(conn);
		//pgDataLoader.load();
	}
}

class PGConnector
{
	private static String IP = "10.32.216.106";
	private static String port = "12345";
	private static String user = "postgres";
	private static String password = "";
	
	static Connection conn = null;
	
	public static Connection getConnection(String db)
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