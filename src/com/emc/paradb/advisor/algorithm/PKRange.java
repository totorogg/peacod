package com.emc.paradb.advisor.algorithm;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.emc.paradb.advisor.data_loader.DBData;
import com.emc.paradb.advisor.data_loader.TableNode;
import com.emc.paradb.advisor.plugin.KeyValuePair;
import com.emc.paradb.advisor.plugin.PlugInterface;
import com.emc.paradb.advisor.utils.QueryPrepare;
import com.emc.paradb.advisor.workload_loader.Transaction;
import com.emc.paradb.advisor.workload_loader.WhereKey.Range;
import com.emc.paradb.advisor.workload_loader.Workload;

/**
 * This algorithm select primary key as partition key
 * Then partition the values by range
 * 
 * @author Xin Pan
 *
 */
public class PKRange implements PlugInterface
{
	Connection conn = null;
	Workload<Transaction<Object>> workload = null;
	DBData dbData = null;
	int nodes = 0;
	List<String[]> paraList = null;
	HashMap<String, List<String>> tableKeyMap = null;
	HashMap<String, LookUpTable> keyLookUpMap = null;
	
	@Override
	public boolean accept(Connection conn, Workload<Transaction<Object>> workload, DBData dbData, int nodes) 
	{
		this.conn = conn;
		this.workload = workload;
		this.dbData = dbData;
		this.nodes = nodes;
		
		tableKeyMap = new HashMap<String, List<String>>();
		keyLookUpMap = new HashMap<String, LookUpTable>();
		
		setPartitionKey();
		setPlacement();
		return true;
	}
	
	private void setPartitionKey()
	{
		HashMap<String, TableNode> tableMap = dbData.getMetaData();
		for(String table : tableMap.keySet())
		{
			TableNode tableNode = tableMap.get(table);
			List<String> primaryKey = tableNode.getPrimaryKey();
			List<String> keys = new ArrayList<String>();
			if(primaryKey.size() > 0)
				keys.add(primaryKey.get(0));
			else
				keys.add(tableNode.getAttrVector().get(0).getName());
			tableKeyMap.put(table, keys);
		}
	}

	private void setPlacement()
	{
		HashMap<String, TableNode> tables = dbData.getMetaData();
		
		for(String tableName :  tableKeyMap.keySet())
		{
			String key = tableKeyMap.get(tableName).get(0);
			
			Statement stmt;
			try 
			{
				stmt = conn.createStatement();
				ResultSet resultMin = stmt.executeQuery("select min("+key+") from "+
										QueryPrepare.prepare(tableName)+";");
				resultMin.next();
				double min = resultMin.getDouble(1);
				
				ResultSet resultMax = stmt.executeQuery("select max("+key+") from "+
										QueryPrepare.prepare(tableName)+";");
				resultMax.next();
				double max = resultMax.getDouble(1);
				
				double range = 0;
				if(paraList != null)
				{
					String rangeString = paraList.get(0)[1];
					try
					{
						range = Double.valueOf(rangeString);
					}catch(NumberFormatException e)
					{
						System.out.println("illegal range value");
						range = 0;
					}
				}
				LookUpTable aLookUp = new LookUpTable(min, max, range, nodes);
				keyLookUpMap.put(tableName, aLookUp);
			} 
			catch (SQLException e) 
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	@Override
	public List<Integer> getNode(List<KeyValuePair> kvPairs) 
	{
		// TODO Auto-generated method stub
		
		Set<Integer> nodes = new HashSet<Integer>();
		
		for(KeyValuePair kvPair : kvPairs)
		{
			if(kvPair.getRange() != Range.EQUAL)
			{
				List<Integer> nodeList = new ArrayList<Integer>();
				nodeList.add(-1);
				return nodeList;
			}
			LookUpTable aLookUp = keyLookUpMap.get(kvPair.getTable());
			aLookUp.updateTable(kvPair);
			nodes.add(aLookUp.getNode(kvPair));
		}
		List<Integer> nodeList = new ArrayList<Integer>(nodes);
		return nodeList;
	}

	@Override
	public HashMap<String, List<String>> getPartitionKey() {
		// TODO Auto-generated method stub
		return tableKeyMap;
	}

	@Override
	public List<String[]> getSetting() {
		// TODO Auto-generated method stub
		paraList = new ArrayList<String[]>();
	
		String[] range = new String[]{"range size(double):","",
				"tables are broken into several equal ranges "};  
		paraList.add(range);
		
		return paraList;
	}
}

class LookUpTable
{
	private double start;
	private double end;
	private double interval;
	private double range = 0;
	private int slots;
	
	public LookUpTable(double start, double end, double range, int slots)
	{
		if(start >= end)
			System.out.println("end >= start");
		
		this.start = start;
		this.end = end;
		this.slots = slots;
		this.range = range;
	}

	
	public int getNode(KeyValuePair kvPair)
	{
		if(kvPair.getValue() == null)
			return -1;
		
		double s = Double.valueOf(kvPair.getValue());
		if (s < start || s > end) 
			return -1;
		
		if(range == 0)
			interval = (end - start) / slots;
		else
			interval = range;
		
		int startNode = (int)((s - start) / interval) % slots;
		
		return startNode;
	}
	
	public void updateTable(KeyValuePair kvPair)
	{
		if(kvPair.getOpera().equalsIgnoreCase("select"))
			return;
		else if(kvPair.getOpera().equalsIgnoreCase("insert") )
			insertTuple(kvPair);
		//we don't consider delete condition now
		//else if(kvPair.getOpera().equalsIgnoreCase("delete") )
		//	deleteTuple(kvPair);
	}
	
	private void insertTuple(KeyValuePair kvPair)
	{
		double value = Double.valueOf(kvPair.getValue());
		if(value < start)
			start = value;
		else if(value > end)
			end = value;
	}
}







