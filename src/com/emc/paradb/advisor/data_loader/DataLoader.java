package com.emc.paradb.advisor.data_loader;

import java.sql.Connection;
import java.sql.DriverManager;


public abstract class DataLoader
{
	protected DBData dbData = new DBData();
	protected Connection conn = null;
	protected float progress = 0;
	protected String selectedBM = null;
	
	public DataLoader(String selectedBM)
	{	
		this.selectedBM = selectedBM;
	}
	public Connection getConn()
	{
		return conn;
	}
	public DBData getDBData()
	{
		return dbData;
	}
	public abstract float getProgress();
}


