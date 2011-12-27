package com.emc.paradb.advisor.plugin;

import java.sql.Connection;
import java.util.HashMap;

import com.emc.paradb.advisor.data_loader.DBData;
import com.emc.paradb.advisor.workload_loader.Transaction;
import com.emc.paradb.advisor.workload_loader.Workload;


public interface PlugInterface
{	
	public boolean accept(Connection conn, Workload<Transaction<Object>> workload, DBData dbData, int nodes);
	
	//partition methods, plug-in should implement one and only one of the partition method
	//the implemented method should by noted in the xml's <partitionmethod> tag
	public HashMap<String, String> getPartitionKey();
	
	//placement methods, plug-in should implement one and only one of the placement method
	//the implemented method should by noted in the xml's <placementmethod> tag
	public HashMap<KeyValuePair, Integer> getPlacement();
	public int getNode();
}



