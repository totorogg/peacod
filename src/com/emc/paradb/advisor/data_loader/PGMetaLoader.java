package com.emc.paradb.advisor.data_loader;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Vector;




class PGMetaLoader
{
	private float progress = 0;
	private Connection conn = null;
	//all the tables in the schemas
	private Vector<TableNode> tables = null;
	
	
	
	public PGMetaLoader(Connection conn, Vector<TableNode> tables)
	{
		this.tables = tables;
		this.conn = conn;
	}
	public Vector<TableNode> getMetaData()
	{
		return tables;
	}
	public float getProgress()
	{
		return progress;
	}
	
	public void load()
	{
		new Thread()
		{
			public void run()
			{
				try
				{
					Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
					//select the reference table and the referenced table;
					ResultSet result = stmt.executeQuery
								 ("select t1.relname as ref, t2.relname as refed from pg_class as t1, pg_class as t2, (select conrelid as oid, confrelid as roid from pg_constraint where contype='f') as t3 where t1.oid = t3.oid and t2.oid = t3.roid;");
					
					result.last();
					int rowCount = result.getRow();
					result.beforeFirst();
				
					while(result.next())
					{
						progress = (float)result.getRow()/rowCount;
						
						String refTable = result.getString(1);
						String refedTable = result.getString(2);
						
						TableNode refNode = new TableNode(refTable, conn);
						TableNode refedNode = new TableNode(refedTable, conn);
						
						//get FK reference keys for the two tables
						Set<String> refKey = new HashSet<String>();
						Set<String> refedKey = new HashSet<String>();
						//adjacentListNode (refedTableNode<TableNode>, refKeys<Set<String>>, refedKeys)
						Vector<Object> adjacentListNode = getFKRef(refNode, refedNode);
						
						//store the reference as adjacent list
						if(!tables.contains(refNode))
						{
							tables.add(refNode);
							refNode.addRefed(adjacentListNode);
						}
						else
							tables.get(tables.indexOf(refNode)).addRefed(adjacentListNode);
							
						if(!tables.contains(refedNode))
							tables.add(refedNode);
					}
					progress = 1;
				}
				catch(SQLException e)
				{
					System.out.println(e.getMessage());
				}
			}
		}.start();
	}


	public Vector<TableNode> getTables() {
		return tables;
	}

	public int getSize() {
		return tables.size();
	}

	public Vector<Object> getFKRef(TableNode refNode, TableNode refedNode) 
	{
		ResultSet result = null;
		Vector<Object> adjacentListNode = new Vector<Object>();
		Set<String> refKeys = new HashSet<String>();
		Set<String> refedKeys = new HashSet<String>();
		

		try {
			Statement stmt = conn.createStatement();
			result = stmt
					.executeQuery("select conkey, confkey "
							+ "from "
							+ "(select t1.relname as ref, conkey, t2.relname as refed, confkey "
							+ "from pg_class as t1, "
							+ "pg_class as t2, "
							+ "(select conrelid as oid, confrelid as roid, conkey, confkey from pg_constraint where contype='f') as t3 "
							+ "where t1.oid = t3.oid and "
							+ "t2.oid = t3.roid) as result " + "where ref='"
							+ refNode.getName() + "' and " + "refed='" + refedNode.getName()
							+ "';");
			result.next();
			Array refArray = result.getArray(1);
			Array refedArray = result.getArray(2);

			Integer[] refAttrNum = (Integer[]) refArray.getArray();
			Integer[] refedAttrNum = (Integer[]) refedArray.getArray();

			for (Integer atnum : refAttrNum) {
				ResultSet attr = stmt
						.executeQuery("select attname "
								+ "from pg_attribute "
								+ "where attrelid = (select oid from pg_class where relname='"
								+ refNode.getName() + "') and attnum ="
								+ atnum.toString() + ";");
				attr.next();
				refKeys.add(attr.getString(1));
			}

			for (Integer atnum : refedAttrNum) {
				ResultSet attr = stmt
						.executeQuery("select attname "
								+ "from pg_attribute "
								+ "where attrelid = (select oid from pg_class where relname='"
								+ refedNode.getName() + "') and attnum ="
								+ atnum.toString() + ";");
				attr.next();
				refedKeys.add(attr.getString(1));
			}
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		
		adjacentListNode.add(refedNode);
		adjacentListNode.add(refKeys);
		adjacentListNode.add(refedKeys);
		
		return adjacentListNode;
	}
}




