package com.emc.paradb.advisor.data_loader;

import java.util.HashMap;
import java.util.Vector;

public class DBData
{
	private HashMap<String, TableNode> tables = new HashMap<String, TableNode>();
	
	public HashMap<String, TableNode> getMetaData()
	{
		return tables;
	}
	
	public void setMeta(HashMap<String, TableNode> tables)
	{
		this.tables = tables;
	}
}