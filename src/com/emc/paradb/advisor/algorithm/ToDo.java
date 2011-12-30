package com.emc.paradb.advisor.algorithm;

import java.sql.Connection;
import java.util.HashMap;

import com.emc.paradb.advisor.data_loader.DBData;
import com.emc.paradb.advisor.plugin.KeyValuePair;
import com.emc.paradb.advisor.plugin.PlugInterface;
import com.emc.paradb.advisor.workload_loader.Transaction;
import com.emc.paradb.advisor.workload_loader.Workload;


public class ToDo extends NaiveAlgorithm
{

	@Override
	public boolean accept(Connection conn,
			Workload<Transaction<Object>> workload, DBData dbData, int nodes) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public HashMap<String, String> getPartitionKey() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public HashMap<KeyValuePair, Integer> getPlacement() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getNode() {
		// TODO Auto-generated method stub
		return 0;
	}
	
}