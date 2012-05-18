package com.emc.paradb.advisor.evaluator;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.emc.paradb.advisor.algorithm.RoundRobin;
import com.emc.paradb.advisor.controller.Controller;
import com.emc.paradb.advisor.data_loader.DataLoader;
import com.emc.paradb.advisor.plugin.KeyValuePair;
import com.emc.paradb.advisor.plugin.PlugInterface;
import com.emc.paradb.advisor.plugin.Plugin;
import com.emc.paradb.advisor.utils.QueryPrepare;

/**
 * this class evaluate data distribution 
 * 
 * 
 * @author Xin Pan
 *
 */
public class DataDistributionEva extends Evaluator
{
	//map from a table name to its partition keys
	private static HashMap<String, List<String>> tableKeyMap = null;
	private static Plugin aPlugin = null;
	
	//start evaluate the algorithm, plugin should contain the
	//algorithms to be evaluated.
	public static List<Long> evaluate(Plugin aPlugin, int nodes)
	{
		List<Long> dataSet = null;
		tableKeyMap = new HashMap<String, List<String>>();
		DataDistributionEva.aPlugin = aPlugin;
		
		//get partition method. Currently, we only support the getPartitionKey() way
		String partitionMD = aPlugin.getPartitionMethod();
		//get placement method. Currently, we only support the getNode() way
		String placementMD = aPlugin.getPlacementMethod();
		
		//get the table and its partition key
		if(partitionMD.equals("getPartitionKey()"))
		{
			tableKeyMap = getPartitionKeyEva();
		}
		else
		{
			try 
			{
				throw new Exception("Undefined partition method");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
		}
		
		//the data placement
		if(placementMD.equals("getNode()"))
		{
			dataSet = getNodeEva();
		}
		else
		{
			try 
			{
				throw new Exception("Undefined placement method");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
		}
		return dataSet;
	}
	
	//get the partition keys from algorithms
	protected static HashMap<String, List<String>> getPartitionKeyEva()
	{
		return aPlugin.getInstance().getPartitionKey();
	}
	//get the node placement result from algorithms
	protected static List<Long> getNodeEva()
	{
		int nodes = Controller.getNodes();
		List<Long> dataSet = new ArrayList<Long>(nodes);
		PlugInterface plugInterface = aPlugin.getInstance();
		
		for(int i = 0; i < nodes; i++)
			dataSet.add(i, 0L);
		
		//data can be partitioned into three ways, the
		//first one is partition by keys
		//second one is replication
		//the third way is undefined in case of which we won't
		//display data distribution results.
		//currently, most algorithm use the first and second partition ways.
		//the third partition ways are only for minTermGraph
		Connection conn = DataLoader.getConn();
		try
		{
			//iterate through each table and get its distribution info
			for(String table : tableKeyMap.keySet())
			{
				List<String> keys = tableKeyMap.get(table);
				Statement stmt = conn.createStatement();
				ResultSet result = null;
				//determine whether it is partitioned by key or replicate, undefined.
				//different partition way needs different routine to handle it
				if(keys.get(0).equalsIgnoreCase("replicate"))
				{
					try {
						result = stmt.executeQuery("select count(*) from " +QueryPrepare.prepare(table));
						result.next();
						for(int i = 0; i < nodes; i++)
							dataSet.set(i, dataSet.get(i) + result.getInt(1));
					} catch (Exception e) {
						System.out.println("replication error, table = " + table + " not found");
					}
				}
				else if(keys.get(0).equals("undefined"))
				{
					List<KeyValuePair> kvPairs = new ArrayList<KeyValuePair>();
					KeyValuePair kvPair = new KeyValuePair(table, "undefined", "undefined");		
					kvPairs.add(kvPair);
					List<Integer> nodeList = plugInterface.getNode(kvPairs);
					
					if(nodeList == null)
						System.err.println("Error: unable to find placement");
					
					for(int i = 0; i < nodeList.size(); i++)
						dataSet.set(i, Long.valueOf(nodeList.get(i)));
				}
				else
				{
					String keyList = keys.get(0);
					for(int i = 1; i < keys.size(); i++)
						keyList = keyList + "," + keys.get(i);
					
					result = stmt.executeQuery("select "+keyList+", count(*) "+
													 "from "+QueryPrepare.prepare(table)+
													 " group by "+keyList+" order by "+keyList+";");
					while(result.next())
					{
						List<KeyValuePair> kvPairs = new ArrayList<KeyValuePair>();
						for(int i = 0; i < keys.size(); i++)
						{
							KeyValuePair kvPair = new KeyValuePair(table, keys.get(i), result.getString(i + 1));
							kvPair.setOpera("select");
							kvPairs.add(kvPair);
						}
						long tuples = result.getInt(keys.size() + 1);

						List<Integer> nodeList = plugInterface.getNode(kvPairs);
						
						for(Integer node : nodeList)
						{
							if(node < 0 || node >= nodes)
							{
								System.out.println("Error: unable to find placement");
								continue;
							}
								
							dataSet.set(node, dataSet.get(node) + tuples);
						}
					}
				}
			}
		}
		catch(SQLException e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
			return null;
		}
		return dataSet;
	}
}