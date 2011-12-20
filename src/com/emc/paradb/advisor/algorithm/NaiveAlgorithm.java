package com.emc.paradb.advisor.algorithm;

import java.util.HashMap;
import java.util.Vector;

import com.emc.paradb.advisor.data_loader.DBData;
import com.emc.paradb.advisor.data_loader.TableAttributes;
import com.emc.paradb.advisor.data_loader.TableNode;
import com.emc.paradb.advisor.plugin.KeyValuePair;
import com.emc.paradb.advisor.plugin.PlugInterface;
import com.emc.paradb.advisor.workload_loader.Transaction;
import com.emc.paradb.advisor.workload_loader.Workload;

public class NaiveAlgorithm implements PlugInterface
{
	HashMap<String, String> tableKeyMap = new HashMap<String, String>();
	HashMap<KeyValuePair, Integer> keyValueNodeMap = new HashMap<KeyValuePair, Integer>();
	Workload<Transaction> workload = null; 
	DBData dbData = null;
	int node = 0;
	
	@Override
	public boolean accept(Workload<Transaction> arg0, DBData arg1, int arg2) {
		// TODO Auto-generated method stub
		this.workload = arg0;
		this.dbData = arg1;
		this.node = arg2;
		
		Vector<TableNode> tables = dbData.getMetaData();
		for(TableNode table : tables)
		{
			Vector<TableAttributes> attr = table.getAttributes();
			tableKeyMap.put(table.getName(), attr.get(0).getName());
		}
		for(String aKey : tableKeyMap.values())
		{
			for(int i = 0; i < 10; i++)
			{
				KeyValuePair kvPair = new KeyValuePair(aKey, String.valueOf(i));
				keyValueNodeMap.put(kvPair, i);
			}
		}
		return true;
	}

	@Override
	public HashMap<String, String> getPartitionKey() {
		// TODO Auto-generated method stub
		return tableKeyMap;
	}

	@Override
	public HashMap<KeyValuePair, Integer> getPlacement() {
		// TODO Auto-generated method stub
		return keyValueNodeMap;
	}
	
}