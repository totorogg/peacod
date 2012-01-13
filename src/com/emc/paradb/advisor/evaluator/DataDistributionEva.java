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
	private static HashMap<String, String> tableKeyMap = null;
	private static Plugin aPlugin = null;
	
	public static List<Long> evaluate(Plugin aPlugin, int nodes)
	{
		List<Long> dataSet = null;
		tableKeyMap = new HashMap<String, String>();
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
	
	protected static HashMap<String, String> getPartitionKeyEva()
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
				String key = tableKeyMap.get(table);
				Statement stmt = conn.createStatement();
				ResultSet result = null;
				
				if(key.equalsIgnoreCase("replicate"))
				{
					result = stmt.executeQuery("select count(*) from " +table);
					result.next();
					for(int i = 0; i < nodes; i++)
						dataSet.set(i, dataSet.get(i) + result.getInt(1));
				}
				else
				{
					result = stmt.executeQuery("select "+key+", count(*) "+
													 "from "+table+
													 " group by "+key+" order by "+key+";");
					while(result.next())
					{
						long tuples = result.getInt(2);
						String value = result.getString(1);
						int node = plugInterface.getNode(new KeyValuePair(table, key, value));
						dataSet.set(node, dataSet.get(node) + tuples);
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