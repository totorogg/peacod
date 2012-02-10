package com.emc.paradb.advisor.data_loader;

import java.sql.Connection;
import java.sql.DriverManager;

import com.emc.paradb.advisor.controller.Controller;
import com.emc.paradb.advisor.controller.PrepareController;



public class PostgreSQLLoader extends DataLoader
{
	private PGMetaLoader pgMetaLoader = null;
	
	public PostgreSQLLoader(String selectedBM) throws Exception
	{
		conn = PGConnector.getConnection();
	}
	
	public float getProgress()
	{
		return pgMetaLoader.getProgress();
	}
	public void load()
	{
		pgMetaLoader = new PGMetaLoader(conn, dbData.getMetaData());
		pgMetaLoader.load();
		//PGDataLoader pgDataLoader = new PGDataLoader(conn);
		//pgDataLoader.load();
	}
}

class PGConnector
{

	public static Connection getConnection()
	{		
		Connection conn = null;
		
		String db = PrepareController.getDBName();
		String ip = PrepareController.getDBIP();
		String port = PrepareController.getDBPort();
		String user = PrepareController.getDBUser();
		String password = PrepareController.getDBPassword();
		
		
		
		try
		{
			Class.forName("org.postgresql.Driver").newInstance();
			conn = DriverManager.getConnection(
					String.format("jdbc:postgresql://%s:%s/%s", ip, port, db), user, password);
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
			e.getStackTrace();
		}
		return conn;
	}
}