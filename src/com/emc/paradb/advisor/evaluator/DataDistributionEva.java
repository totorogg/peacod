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


public class DataDistributionEva extends Evaluator
{
	private static HashMap<String, List<String>> tableKeyMap = null;
	private static Plugin aPlugin = null;
	
	public static List<Long> evaluate(Plugin aPlugin, int nodes)
	{
		List<Long> dataSet = null;
		tableKeyMap = new HashMap<String, List<String>>();
		DataDistributionEva.aPlugin = aPlugin;
		
		String partitionMD = aPlugin.getPartitionMethod();
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
	
	protected static HashMap<String, List<String>> getPartitionKeyEva()
	{
		return aPlugin.getInstance().getPartitionKey();
	}
	
	protected static List<Long> getNodeEva()
	{
		int nodes = Controller.getNodes();
		List<Long> dataSet = new ArrayList<Long>(nodes);
		PlugInterface plugInterface = aPlugin.getInstance();
		
		for(int i = 0; i < nodes; i++)
			dataSet.add(i, 0L);
		
		Connection conn = DataLoader.getConn();
		try
		{
			for(String table : tableKeyMap.keySet())
			{
				List<String> keys = tableKeyMap.get(table);
				Statement stmt = conn.createStatement();
				ResultSet result = null;
				
				if(keys.get(0).equalsIgnoreCase("replicate"))
				{
					result = stmt.executeQuery("select count(*) from " +table);
					result.next();
					for(int i = 0; i < nodes; i++)
						dataSet.set(i, dataSet.get(i) + result.getInt(1));
				}
				else
				{
					String keyList = keys.get(0);
					for(int i = 1; i < keys.size(); i++)
						keyList = keyList + "," + keys.get(i);
					
					result = stmt.executeQuery("select "+keyList+", count(*) "+
													 "from "+table+
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