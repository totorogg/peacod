package com.emc.paradb.advisor.plugin;

import java.sql.Connection;
import java.util.HashMap;

import com.emc.paradb.advisor.data_loader.DBData;
import com.emc.paradb.advisor.workload_loader.Transaction;
import com.emc.paradb.advisor.workload_loader.Workload;


public interface PlugInterface
{	
	public boolean accept(Connection conn, Workload<Transaction<Object>> workload, DBData dbData, int nodes);
	public HashMap<KeyValuePair, Integer> getPlacement();
	public HashMap<String, String> getPartitionKey();
}



