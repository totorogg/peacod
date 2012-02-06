package com.emc.paradb.advisor.data_loader;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Vector;




public class TableNode
{
	private Connection conn = null;
	private String name;
	private int length;
	private List<String> primaryKey = null;
	//the attributes corresponding to the table
	private HashMap<String, TableAttributes> tableAttributeMap = new HashMap<String, TableAttributes>();
	private Vector<TableAttributes> attributes = new Vector<TableAttributes>();
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
			while(result.next())
			{
				TableAttributes aAttr = new TableAttributes(newName, result.getString(1), conn);
				tableAttributeMap.put(result.getString(1), aAttr);
				attributes.add(aAttr);
			}
			//get the primary keys
			result = stmt.executeQuery("SELECT "+              
									   "pg_attribute.attname,"+ 
									   "format_type(pg_attribute.atttypid, pg_attribute.atttypmod) "+
									   "FROM pg_index, pg_class, pg_attribute "+
									   "WHERE "+
									   "pg_class.oid = '"+newName+"'::regclass AND "+
									   "indrelid = pg_class.oid AND "+
									   "pg_attribute.attrelid = pg_class.oid AND "+
									   "pg_attribute.attnum = any(pg_index.indkey) "+
									   "AND indisprimary");
			
			primaryKey = new ArrayList<String>();
			while(result.next())
			{
				primaryKey.add(result.getString(1));
			}
			
			//get the table length;
			result = stmt.executeQuery("select count(*) from "+newName+";");
			result.next();
			length = Integer.valueOf(result.getString(1));
			
		}
		catch(SQLException e)
		{
			System.out.println(e.getMessage());
		}
	}
	public Vector<Vector<Object>> getFKRef()
	{
		return FKRef;
	}
	public String getName(){
		return name;
	}
	public int getLength(){
		return length;
	}
	public List<String> getPrimaryKey()
	{
		return primaryKey;
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

	public HashMap<String, TableAttributes> getAttributes(){
		return tableAttributeMap;
	}
	public Vector<TableAttributes> getAttrVector()
	{
		return attributes;
	}

}