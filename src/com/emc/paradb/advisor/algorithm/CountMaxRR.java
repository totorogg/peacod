package com.emc.paradb.advisor.algorithm;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

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

/**
 * This class selects the most frequently accessed key of each table by the workloads
 * It implements the getPartitionKey() and getNode() function
 * @author panx1
 *
 */
public class CountMaxRR implements PlugInterface
{
	Connection conn = null;
	Workload<Transaction<Object>> workload = null;
	DBData dbData = null;
	RoundRobin RR = null;
	int nodes = 0;
	
	HashMap<String, List<String>> tableKeyMap = null;
	
	@Override
	public boolean accept(Connection conn, Workload<Transaction<Object>> workload,
			DBData dbData, int nodes) {

		this.conn = conn;
		this.workload = workload;
		this.dbData = dbData;
		this.nodes = nodes;
		tableKeyMap = new HashMap<String, List<String>>();
		
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
	public HashMap<String, List<String>> getPartitionKey() {
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
						updateTableKeyCount(tableKeyCount, key.getTableName(), key.getKeyName());	
					}
				}
				else if(statement instanceof UpdateAnalysisInfo)
				{
					UpdateAnalysisInfo update = (UpdateAnalysisInfo)statement;
					for(WhereKey key : update.getWhereKeys())
					{
						updateTableKeyCount(tableKeyCount, key.getTableName(), key.getKeyName());	
					}
				}
				else if(statement instanceof InsertAnalysisInfo)
				{
					InsertAnalysisInfo insert = (InsertAnalysisInfo)statement;
					
					for(String key : insert.getKeyValueMap().keySet())
						updateTableKeyCount(tableKeyCount, insert.getTable(), key);
				}
				else if(statement instanceof DeleteAnalysisInfo)
				{
					DeleteAnalysisInfo delete = (DeleteAnalysisInfo)statement;
					for(WhereKey key : delete.getWhereKeys())
						updateTableKeyCount(tableKeyCount, key.getTableName(), key.getKeyName());	
				}
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
			List<String> keys = new ArrayList<String>();
			keys.add(partitionKey);
			tableKeyMap.put(table, keys);
		}
	}
	
	protected void updateTableKeyCount(HashMap<String, HashMap<String, Integer>> tableKeyCount, String table, String key) throws Exception
	{	
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

	@Override
	/**
	 * this function simply return a int number per call. the number will increase by one every call.
	 */
	public List<Integer> getNode(List<KeyValuePair> kvPairs) 
	{
		// TODO Auto-generated method stub
		String value = kvPairs.get(0).getValue();
		List<Integer> nodes = new ArrayList<Integer>();
		
		if(value != null)
			nodes.add(RR.getPlacement(value));
		else
			nodes.add(-1);
		
		return nodes;
	}
}