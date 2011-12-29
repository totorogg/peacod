package com.emc.paradb.advisor.algorithm;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

import com.emc.paradb.advisor.data_loader.DBData;
import com.emc.paradb.advisor.data_loader.TableNode;
import com.emc.paradb.advisor.plugin.KeyValuePair;
import com.emc.paradb.advisor.plugin.PlugInterface;
import com.emc.paradb.advisor.workload_loader.Transaction;
import com.emc.paradb.advisor.workload_loader.Workload;


public class SchemaHash implements PlugInterface
{
	Connection conn = null;
	Workload<Transaction<Object>> workload = null;
	DBData dbData = null;
	RoundRobin RR = null;
	int nodes = 0;
	
	HashMap<KeyValuePair, Integer> kvnMap = new HashMap<KeyValuePair, Integer>();
	HashMap<String, String> tableKeyMap = new HashMap<String, String>();
	
	@Override
	public boolean accept(Connection conn, Workload<Transaction<Object>> workload,
			DBData dbData, int nodes) {

		this.conn = conn;
		this.workload = workload;
		this.dbData = dbData;
		this.nodes = nodes;
		
		try
		{
			setPartition();
			setPlacement();
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		return true;
	}
	
	protected void setPartition()
	{
		HashMap<String, TableNode> tables = dbData.getMetaData();
		Vector<Object> influenceNode = null;
		HashMap<String, Vector<Object>> influenceList = null;
		HashMap<String, HashMap<String, Vector<Object>>> influenceTree = 
				new HashMap<String, HashMap<String, Vector<Object>>>();
		
		
		for(TableNode table : tables.values())
		{
			Vector<Vector<Object>> fkRef = table.getFKRef();
			
			if(fkRef == null || fkRef.size() == 0)
			{
				String partitionKey = table.getAttrVector().get(0).getName();
				tableKeyMap.put(table.getName(), partitionKey);
				continue;
			}
			
			for(Vector<Object> refedNode : fkRef)
			{
				TableNode refed = (TableNode)refedNode.get(0);
				if(influenceTree.get(refed.getName()) == null)
				{
					influenceList = new HashMap<String, Vector<Object>>();
					influenceTree.put(refed.getName(), influenceList);
				}
				else
					influenceList = influenceTree.get(refed.getName());
				
				influenceNode = new Vector<Object>();
				influenceNode.add(refedNode.get(2));
				influenceNode.add(refedNode.get(1));
				influenceList.put(table.getName(), influenceNode);
			}
		}
		
		while( influenceTree.size() > 0)
		{
			String startTable = null;
			for(String table : influenceTree.keySet())
			{
				int max = Integer.MIN_VALUE;
				if(influenceTree.get(table).size() > max)
				{
					startTable = table;
					max = influenceTree.get(table).size();
				}
			}
			influenceList = influenceTree.get(startTable);
			if(influenceList.size() < 1)
				break;
			for(String table : influenceList.keySet())
			{
				if(tableKeyMap.get(table) == null)
				{	
					List<String> refedKeySet= (List<String>)influenceList.get(table).get(1);
					getKeyRecursive(table, refedKeySet.get(0), influenceTree);
				}
			}
			influenceTree.remove(startTable);
		}
	}
	
	private void getKeyRecursive(String startTable, String partitionKey, HashMap<String, HashMap<String, Vector<Object>>> influenceTree)
	{
		HashMap<String, Vector<Object>> influenceList = influenceTree.get(startTable);
		
		tableKeyMap.put(startTable, partitionKey);
		if(influenceList != null && influenceList.size() > 0)
		{
			for(String table : influenceList.keySet())
			{
				if(tableKeyMap.get(table) == null)
				{	
					List<String> refedKeySet= (List<String>)influenceList.get(table).get(1);
					getKeyRecursive(table, refedKeySet.get(0), influenceTree);
				}
			}
		}
		influenceTree.remove(startTable);
	}
	@Override
	public HashMap<String, String> getPartitionKey() {
		// TODO Auto-generated method stub
		return tableKeyMap;
	}

	protected void setPlacement()
	{
		HashBased hash = new HashBased(nodes);
		
		for(String table : tableKeyMap.keySet())
		{
			String key = tableKeyMap.get(table);
			List<KeyValuePair> keyValues = getKeyValuePair(table, key);
			for(KeyValuePair kvPair : keyValues)
				kvnMap.put(kvPair, hash.getPlacement(kvPair.getValue()));
		}
	}
	protected List<KeyValuePair> getKeyValuePair(String table, String key)
	{
		List<KeyValuePair> keyValueList = new ArrayList<KeyValuePair>();
		try
		{
			Statement stmt = conn.createStatement();
			ResultSet result = stmt.executeQuery(String.format("select %s from %s group by %s", key, table, key));
			while(result.next())
			{
				KeyValuePair kvPair = new KeyValuePair(key, result.getString(1));
				keyValueList.add(kvPair);
			}
		}
		catch(SQLException e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		return keyValueList;
	}
	@Override
	public HashMap<KeyValuePair, Integer> getPlacement() {
		// TODO Auto-generated method stub
		return kvnMap;
	}

	@Override
	public int getNode() {
		// TODO Auto-generated method stub
		return 0;
	}
}
