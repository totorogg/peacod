package com.emc.paradb.advisor.algorithm;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.emc.paradb.advisor.data_loader.DBData;
import com.emc.paradb.advisor.data_loader.TableNode;
import com.emc.paradb.advisor.plugin.KeyValuePair;
import com.emc.paradb.advisor.plugin.KeyValuePair.Range;
import com.emc.paradb.advisor.plugin.PlugInterface;
import com.emc.paradb.advisor.workload_loader.Transaction;
import com.emc.paradb.advisor.workload_loader.Workload;

/**
 * we replicate all tables to each node 
 * and palce workload to nodes in Hash manner
 * 
 * 
 * @author panx1
 */
public class AllReplicateHash implements PlugInterface
{
	Connection conn = null;
	Workload<Transaction<Object>> workload = null;
	DBData dbData = null;
	HashBased hash = null;
	int nodes = 0;
	
	HashMap<String, List<String>> tableKeyMap = null;
	
	@Override
	public boolean accept(Connection conn, Workload<Transaction<Object>> workload, DBData dbData, int nodes) 
	{
		this.conn = conn;
		this.workload = workload;
		this.dbData = dbData;
		this.nodes = nodes;
		tableKeyMap = new HashMap<String, List<String>>();

		setPartitionKey();
		hash = new HashBased(nodes);
		
		return true;
	}

	public void setPartitionKey()
	{
		List<String> keyList = new ArrayList<String>();
		keyList.add("replicate");
		for(String table : dbData.getMetaData().keySet())
			tableKeyMap.put(table, keyList);
	}
	@Override
	public HashMap<String, List<String>> getPartitionKey() {
		// TODO Auto-generated method stub
		return tableKeyMap;
	}

	@Override
	public List<Integer> getNode(List<KeyValuePair> kvPairs) 
	{
		// TODO Auto-generated method stub
		List<Integer> nodes = new ArrayList<Integer>();
		if(kvPairs.get(0).getRange() != Range.EQUAL)
		{
			nodes.add(-1);
			return nodes;
		}
		
		String value = kvPairs.get(0).getValue();		
		if(value != null)
			nodes.add(hash.getPlacement(value));
		else
			nodes.add(-1);
		
		return nodes;
	}

	@Override
	public List<String[]> getSetting() {
		// TODO Auto-generated method stub
		return null;
	}
}