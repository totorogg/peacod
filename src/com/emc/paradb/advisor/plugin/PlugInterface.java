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
	//note that it return -1 to indicate it cannot get node number;
	//note that it return -2 to indicate every node is OK (replicated)
	public int getNode(KeyValuePair kvPair);
	
	//insert methods, plug-in should implement one insert method, return the node to insert
	//note that it return -1 to indicate it cannot get node number;
	//note that it return -2 to indicate every node is OK (replicated)
	public int insert(KeyValuePair kvPair);
	
	//insert methods, plug-in should implement one remove method, return the node to remove
	//note that it return -1 to indicate it cannot get node number;
	//note that it return -2 to indicate every node is OK (replicated)
	public int remove(KeyValuePair kvPair);
}



