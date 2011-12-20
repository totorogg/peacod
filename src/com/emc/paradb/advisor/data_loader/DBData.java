package com.emc.paradb.advisor.data_loader;

import java.util.Vector;

public class DBData
{
	private Vector<TableNode> tables = new Vector<TableNode>();
	
	public Vector<TableNode> getMetaData()
	{
		return tables;
	}
	public void setMeta(Vector<TableNode> tables)
	{
		this.tables = tables;
	}
	public Vector<TableNode> getMeta()
	{
		return tables;
	}
}