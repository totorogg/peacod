package com.emc.paradb.advisor.algorithm;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Vector;

import com.emc.paradb.advisor.data_loader.DBData;
import com.emc.paradb.advisor.data_loader.TableNode;
import com.emc.paradb.advisor.plugin.KeyValuePair;
import com.emc.paradb.advisor.plugin.PlugInterface;
import com.emc.paradb.advisor.workload_loader.Transaction;
import com.emc.paradb.advisor.workload_loader.Workload;

/**
 * this class select a base table first. The base table should be a most influential table.
 * If table A references table B, we said table B has influentiality 1.
 * If another table references A, than B should have influentiality 2.
 * e.g. warehouse table has the highest influentiality.
 * 
 * then it partition the base table by its primary key. all other tables reference it
 * are partitioned by the foreign keys referencing its parent table (e.g. base table)
 * 
 * this class implements the getPartitionKey and getPlacement.
 * therefore, it should know a value of a key goes to which node.
 * 
 * Note: if primary key is composed of several keys, we pick the first one
 * 
 * @author panx1
 *
 */
public class SchemaHash implements PlugInterface
{
	Connection conn = null;
	Workload<Transaction<Object>> workload = null;
	DBData dbData = null;
	HashBased hash = null;
	int nodes = 0;
	
	HashMap<String, List<String>> tableKeyMap = new HashMap<String, List<String>>();
	
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
			hash = new HashBased(nodes);
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		return true;
	}
	
	//set the partition, record them in tableKeyMap
	protected void setPartition()
	{
		Vector<Object> influenceNode = null;
		HashMap<String, TableNode> tables = dbData.getMetaData();
		HashMap<String, Vector<Object>> influenceList = null;
		HashMap<String, HashMap<String, Vector<Object>>> influenceTree = 
				new HashMap<String, HashMap<String, Vector<Object>>>();
		
		
		for(String table : tables.keySet())
			influenceTree.put(table, new HashMap<String, Vector<Object>>());
		
		for(TableNode table : tables.values())
		{
			Vector<Vector<Object>> fkRef = table.getFKRef();
			
			if(fkRef == null || fkRef.size() == 0)
			{
				String partitionKey = table.getAttrVector().get(0).getName();
				List<String> keys = new ArrayList<String>();
				keys.add(partitionKey);
				tableKeyMap.put(table.getName(), keys);
				continue;
			}
			
			for(Vector<Object> refedNode : fkRef)
			{
				TableNode refed = (TableNode)refedNode.get(0);
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
			int max = Integer.MIN_VALUE;
			for(String table : influenceTree.keySet())
			{
				int size = getInfluence(table, influenceTree);	
				if(size > max)
				{
					startTable = table;
					max = size;
				}
			}
			//here we choose the first attribute, we should select the primary key in future work
			List<String> keys = new ArrayList<String>();
			keys.add(tables.get(startTable).getAttrVector().get(0).getName());
			tableKeyMap.put(startTable, keys);
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
		List<String> keys = new ArrayList<String>();
		keys.add("replicate");
		tableKeyMap.put("item", keys);
	}
	private int getInfluence(String table, HashMap<String, HashMap<String, Vector<Object>>> influenceTree)
	{
		HashMap<String, Vector<Object>> influenceList = influenceTree.get(table);
		int descendants = 1;
		
		if(influenceList == null || influenceList.size() == 0)
			return descendants;
		
		for(String child : influenceList.keySet())
			descendants += getInfluence(child, influenceTree);
		
		return descendants;
	}
	private void getKeyRecursive(String startTable, String partitionKey, HashMap<String, HashMap<String, Vector<Object>>> influenceTree)
	{
		HashMap<String, Vector<Object>> influenceList = influenceTree.get(startTable);
		List<String> keys = new ArrayList<String>();
		keys.add(partitionKey);
		tableKeyMap.put(startTable, keys);
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
	public HashMap<String, List<String>> getPartitionKey() {
		// TODO Auto-generated method stub
		return tableKeyMap;
	}

	protected List<KeyValuePair> getKeyValuePair(String table, String key)
	{
		List<KeyValuePair> keyValueList = new ArrayList<KeyValuePair>();
		try
		{
			Statement stmt = conn.createStatement();
			ResultSet result = stmt.executeQuery(String.format("select %s, count(*) from %s group by %s order by %s", key, table, key, key));
			while(result.next())
			{
				KeyValuePair kvPair = new KeyValuePair(key, result.getString(1), result.getInt(2));
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
	public List<Integer> getNode(List<KeyValuePair> kvPairs) {
		// TODO Auto-generated method stub
		String value = kvPairs.get(0).getValue();
		List<Integer> nodes = new ArrayList<Integer>();
		
		if(value != null)
			nodes.add(hash.getPlacement(value));
		else
			nodes.add(0);
		
		return nodes;
	}
}
