package com.emc.paradb.advisor.plugin;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;

import com.emc.paradb.advisor.data_loader.DBData;
import com.emc.paradb.advisor.workload_loader.Transaction;
import com.emc.paradb.advisor.workload_loader.Workload;

/**
 * it is the core interface of our plugin system
 * every algorithm should implement this interface so as
 * to co-play with our system.
 * System will communicate with algorithms through 
 * this functions defined in this interface
 * 
 * most importantly there are three parts.
 * the accept() funcation for accepting neccessary info for the algorithm
 * such as database connection, workload and database meta
 * the getPartitionKey() function for returning partition keys
 * the getNode() function for retrieving partition results
 * 
 * @author XinPan
 *
 */
public interface PlugInterface
{	
	public boolean accept(Connection conn, 
			Workload<Transaction<Object>> workload, 
			DBData dbData, 
			int nodes);
	
	//partition methods, plug-in should implement one and only one of the partition method
	//the implemented method should by noted in the xml's <partitionmethod> tag
	public HashMap<String, List<String>> getPartitionKey();
	
	//placement methods, plug-in should implement one and only one of the placement method
	//the implemented method should by noted in the xml's <placementmethod> tag
	//note that it return -1 to indicate it cannot get node number;
	//note that it return -2 to indicate every node is OK (replicated)
	public List<Integer> getNode(List<KeyValuePair> kvPairs);
	
	//a list of parameters. Each String[] has three components: key, value, description
	//String[1] is key, provided by the algorithm
	//String[2] is value, provided by the user
	//String[3] is describtion, provided by the algorithm
	//null list means no setting.
	public List<String[]> getSetting();
	
}



