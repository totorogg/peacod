package com.emc.paradb.advisor.data_loader;



import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Set;



public class TableAttributes
{
	private Connection conn = null;
	private String attrName;//name of the attribute
	private int Cardinality;//number of different value the attribute has
	private Set<AttributeValue> values = null;
	
	private class ValueCount<Object, Integer>
	{
		
	}
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
	private Set<AttributeValue> getAttrValue()
	{
		return values;
	}
	private boolean setAttrValue(String tableName)
	{
		values = new HashSet<AttributeValue>();
		try
		{
			Statement stmt = conn.createStatement();
			ResultSet result = stmt.executeQuery(String.format("select %s, count(*) from %s group by %s", attrName, tableName, attrName));
			while(result.next())
			{
				values.add( new AttributeValue(result.getString(1), result.getInt(2)));
			}
		}
		catch(SQLException e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
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
}