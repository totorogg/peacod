package com.emc.paradb.advisor.data_loader;

import java.sql.Connection;
import java.sql.DriverManager;


public abstract class DataLoader
{
	private float progress = 0;
	private String selectedBM = null;
	
	public DataLoader(String selectedBM)
	{	
		this.selectedBM = selectedBM;
	}
	
	public abstract float getProgress();
}


