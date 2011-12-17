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
	private Vector<TableNode> tables = new Vector<TableNode>();
	
	
	
	public PGMetaLoader(Connection conn)
	{
		this.conn = conn;
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


class TableNode
{
	private Connection conn = null;
	private String name;
	private int length;

	//the attributes corresponding to the table
	private Vector<TableAttributes> tableAttributes = new Vector<TableAttributes>();
	//an adjacent list. Each list node contains a refed tableNode, refKeys and refed Keys
	private Vector<Vector<Object>> FKRef = new Vector<Vector<Object>>();
	
	public TableNode(String newName, Connection conn){
		
		this.conn = conn;
		name = newName;
		try{
			Statement stmt = conn.createStatement();
			//get the name of its attributes
			ResultSet result = stmt.executeQuery("SELECT column_name "+
												 "FROM information_schema.columns "+
												 "WHERE table_name ='"+newName+"';");
			while(result.next()){
				tableAttributes.add(new TableAttributes(newName, result.getString(1), conn));
			}
			
			//get the table length;
			result = stmt.executeQuery("select count(*) from "+newName+";");
			result.next();
			length = Integer.valueOf(result.getString(1));
			
		}catch(SQLException e){
			System.out.println(e.getMessage());
		}
	}

	public String getName(){
		return name;
	}
	public int getLength(){
		return length;
	}
	
	public boolean addRefed(Vector<Object> newNode)
	{
		return FKRef.add(newNode);
	}
	
	public boolean deleteRefed(TableNode delNode){
		
		for(int i = 0; i < FKRef.size(); i++)
			if(FKRef.get(i).get(0).equals(delNode))
			{
				FKRef.remove(i);
				return true;
			}
		
		return false;
	}
	

	public boolean equals(Object node)
	{	
		if(this.name.equals( ((TableNode)node).getName()))
			return true;
		else
			return false;
	}

	public Vector<TableAttributes> getAttributes(){
		return tableAttributes;
	}

}

class TableAttributes
{
	private Connection conn = null;
	
	public TableAttributes(String table, String name, Connection conn)
	{	
		this.conn = conn;
		attrName = name;
		setCardinaligy(table, name);
	}
	private boolean setCardinaligy(String table, String name)
	{
		try{
			Statement stmt = conn.createStatement();
			ResultSet result = stmt.executeQuery("select count(distinct("+name+")) from "+table+" as t1;");
			result.next();
			Cardinality = Integer.valueOf(result.getString(1));
			return true;
		}catch(SQLException e){
			System.out.println(e.getMessage());
		}
		return false;
	}
	
	private boolean setColMeta(String table, String col)
	{
		try{
			DatabaseMetaData meta = conn.getMetaData();
			ResultSet result = meta.getColumns(null, null, table, col);
			
			String[][] data = new String[1][6];
			while(result.next()){
				data[0][0] = result.getString(3);
				data[0][1] = result.getString(4);
				data[0][2] = result.getString(6);
				data[0][3] = result.getString(7);
				data[0][4] = result.getString(17);
				data[0][5] = result.getString(18);
			}
		}catch(SQLException e){
			System.out.println(e.getMessage());
			return false;
		}
		return true;
	}

	public int getCardinality(){
		return Cardinality;
	}
	public String getName(){
		return attrName;
	}
	
	private String attrName;//name of the attribute
	private int Cardinality;//number of different value the attribute has
}