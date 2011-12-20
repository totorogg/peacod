package com.emc.paradb.advisor.data_loader;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Vector;




public class TableNode
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