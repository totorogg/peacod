package com.emc.paradb.advisor.algorithm;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.emc.paradb.advisor.data_loader.DBData;
import com.emc.paradb.advisor.data_loader.TableAttributes;
import com.emc.paradb.advisor.data_loader.TableNode;
import com.emc.paradb.advisor.plugin.KeyValuePair;
import com.emc.paradb.advisor.plugin.PlugInterface;
import com.emc.paradb.advisor.workload_loader.DeleteAnalysisInfo;
import com.emc.paradb.advisor.workload_loader.InsertAnalysisInfo;
import com.emc.paradb.advisor.workload_loader.SelectAnalysisInfo;
import com.emc.paradb.advisor.workload_loader.Transaction;
import com.emc.paradb.advisor.workload_loader.UpdateAnalysisInfo;
import com.emc.paradb.advisor.workload_loader.WhereKey;
import com.emc.paradb.advisor.workload_loader.Workload;


public class CountMaxRR implements PlugInterface
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
			setPartitionKey();
			RR = new RoundRobin(nodes);
		} 
		catch (Exception e) 
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}	
		return true;
	}

	@Override
	public HashMap<String, String> getPartitionKey() {
		return tableKeyMap;
	}
	
	public void setPartitionKey() throws Exception
	{
		HashMap<String, HashMap<String, Integer>> tableKeyCount = new HashMap<String, HashMap<String, Integer>>();
		

		for(Transaction<Object> aTran : workload)
		{
			for(Object statement : aTran)
			{
				if(statement instanceof SelectAnalysisInfo)
				{
					SelectAnalysisInfo select = (SelectAnalysisInfo)statement;
					
					for(WhereKey key : select.getWhereKeys())
					{						
						updateTableKeyCount(tableKeyCount, key.getKeyName());	
					}
				}
				else if(statement instanceof UpdateAnalysisInfo)
				{
					UpdateAnalysisInfo update = (UpdateAnalysisInfo)statement;
					for(WhereKey key : update.getWhereKeys())
					{
						updateTableKeyCount(tableKeyCount, key.getKeyName());	
					}
				}
				else if(statement instanceof InsertAnalysisInfo)
				{
					InsertAnalysisInfo insert = (InsertAnalysisInfo)statement;
					
					for(String key : insert.getKeyValueMap().keySet())
						updateTableKeyCount(tableKeyCount, key);
				}
				else if(statement instanceof DeleteAnalysisInfo)
				{
					DeleteAnalysisInfo delete = (DeleteAnalysisInfo)statement;
					for(WhereKey key : delete.getWhereKeys())
						updateTableKeyCount(tableKeyCount, key.getKeyName());	
				}
			}
			
			for(String table : tableKeyCount.keySet())
			{
				int max = Integer.MIN_VALUE;
				String partitionKey = "";
				HashMap<String, Integer> keyCount = tableKeyCount.get(table);
				
				for(String key : keyCount.keySet())
				{
					if(keyCount.get(key) > max)
					{
						partitionKey = key;
						max = keyCount.get(key);
					}
				}
				tableKeyMap.put(table, partitionKey);
			}
		}
	}
	
	protected void updateTableKeyCount(HashMap<String, HashMap<String, Integer>> tableKeyCount, String key) throws Exception
	{
		String table = null;	
		HashMap<String, TableNode> tables = dbData.getMetaData();
		
		for(TableNode tableNode : tables.values())
		{
			HashMap<String, TableAttributes> attributes = tableNode.getAttributes();
			if(attributes.get(key) != null)
			{
				table = tableNode.getName();
				break;
			}
		}
		
		//findTableName:
		if(table == null)
			System.out.println(String.format("%s cannot find %s", table, key));
		
		HashMap<String, Integer> keyCount;
		if(!tableKeyCount.containsKey(table))
		{
			keyCount = new HashMap<String, Integer>();
			tableKeyCount.put(table, keyCount);
		}
		else
			keyCount = tableKeyCount.get(table);
		
		if(keyCount.get(key) == null)
			keyCount.put(key, 1);
		else
			keyCount.put(key, keyCount.get(key) + 1);
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
	/**
	 * this function simply return a int number per call. the number will increase by one every call.
	 */
	public int getNode() {
		// TODO Auto-generated method stub
		return RR.getPlacement();
	}

	@Override
	public HashMap<KeyValuePair, Integer> getPlacement() {
		// TODO Auto-generated method stub
		return null;
	}
}