package com.emc.paradb.advisor.data_loader;

import java.sql.Connection;
import java.sql.DriverManager;


public abstract class DataLoader
{
	protected static DBData dbData = new DBData();
	protected static Connection conn = null;
	protected static float progress = 0;
	protected static String selectedBM = null;
	
	public static void setBenchMark(String selectedBM)
	{	
		DataLoader.selectedBM = selectedBM;
	}
	public static Connection getConn()
	{
		return conn;
	}
	public static DBData getDBData()
	{
		return dbData;
	}
	public abstract float getProgress();
	public abstract void load();
}


