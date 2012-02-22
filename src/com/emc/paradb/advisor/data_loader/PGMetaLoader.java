package com.emc.paradb.advisor.data_loader;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import com.emc.paradb.advisor.utils.QueryPrepare;




class PGMetaLoader
{
	private float progress = 0;
	private Connection conn = null;
	//all the tables in the schemas
	private HashMap<String, TableNode> tables = null;
	
	
	
	public PGMetaLoader(Connection conn, HashMap<String, TableNode> tables)
	{
		this.tables = tables;
		this.conn = conn;
	}
	public HashMap<String, TableNode> getMetaData()
	{
		return tables;
	}
	public float getProgress()
	{
		return progress;
	}
	
	//note that this function has to be adjust to make sure that nodes not participating in fk refenrence are added
	public void load()
	{
		new Thread()
		{
			public void run()
			{
				try
				{
					Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
					//select all the table names
					ResultSet tableResult = stmt.executeQuery("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';");

					tableResult.last();
					int currentPos = 0;
					int rowCount = tableResult.getRow();
					rowCount++;
					tableResult.beforeFirst();
					
					//get all the TableNodes
					while(tableResult.next())
					{
						String name = tableResult.getString(1);
						TableNode aTableNode = new TableNode(QueryPrepare.prepare(name), conn);
						tables.put(QueryPrepare.prepare(name), aTableNode);
						progress = (float)(++currentPos)/rowCount;
					}
					
					//add the FK reference among nodes
					//select the reference table and the referenced table;
					ResultSet result = stmt.executeQuery
								 ("select t1.relname as ref, t2.relname as refed from pg_class as t1, pg_class as t2, (select conrelid as oid, confrelid as roid from pg_constraint where contype='f') as t3 where t1.oid = t3.oid and t2.oid = t3.roid;");
					
					while(result.next())
					{
						String refTable = QueryPrepare.prepare(result.getString(1));
						String refedTable = QueryPrepare.prepare(result.getString(2));
						
						TableNode refNode = tables.get(refTable);
						TableNode refedNode = tables.get(refedTable);
						
						//get FK reference keys for the two tables
						Set<String> refKey = new HashSet<String>();
						Set<String> refedKey = new HashSet<String>();
						//adjacentListNode (refedTableNode<TableNode>, refKeys<Set<String>>, refedKeys)
						Vector<Object> adjacentListNode = getFKRef(refNode, refedNode);						
						refNode.addRefed(adjacentListNode);

					}
					progress = 1;
				}
				catch(SQLException e)
				{
					System.out.println(e.getMessage());
					e.printStackTrace();
				}
			}
		}.start();
	}


	public HashMap<String, TableNode> getTables() {
		return tables;
	}

	public int getSize() {
		return tables.size();
	}

	public Vector<Object> getFKRef(TableNode refNode, TableNode refedNode) 
	{
		ResultSet result = null;
		Vector<Object> adjacentListNode = new Vector<Object>();
		List<String> refKeys = new ArrayList<String>();
		List<String> refedKeys = new ArrayList<String>();
		

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
							+ QueryPrepare.unPrepare(refNode.getName()) + "' and " + "refed='" + QueryPrepare.unPrepare(refedNode.getName())
							+ "';");
			if(result.next())
			{
				Array refArray = result.getArray(1);
				Array refedArray = result.getArray(2);

				Integer[] refAttrNum = (Integer[]) refArray.getArray();
				Integer[] refedAttrNum = (Integer[]) refedArray.getArray();

				for (Integer atnum : refAttrNum) {
					ResultSet attr = stmt
							.executeQuery("select attname "
									+ "from pg_attribute "
									+ "where attrelid = (select oid from pg_class where relname='"
									+ QueryPrepare.unPrepare(refNode.getName()) + "') and attnum ="
									+ QueryPrepare.unPrepare(atnum.toString()) + ";");
					attr.next();
					refKeys.add(attr.getString(1));
				}

				for (Integer atnum : refedAttrNum) {
					ResultSet attr = stmt
							.executeQuery("select attname "
									+ "from pg_attribute "
									+ "where attrelid = (select oid from pg_class where relname='"
									+ QueryPrepare.unPrepare(refedNode.getName()) + "') and attnum ="
									+ QueryPrepare.unPrepare(atnum.toString()) + ";");
					attr.next();
					refedKeys.add(attr.getString(1));
				}
				
				adjacentListNode.add(refedNode);
				adjacentListNode.add(refKeys);
				adjacentListNode.add(refedKeys);
			}
		} catch (SQLException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		

		
		return adjacentListNode;
	}
}




