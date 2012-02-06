package com.emc.paradb.advisor.evaluator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.emc.paradb.advisor.algorithm.AlgorithmFactory;
import com.emc.paradb.advisor.data_loader.DataLoader;
import com.emc.paradb.advisor.data_loader.TableNode;
import com.emc.paradb.advisor.plugin.Plugin;


public class Summarize
{
	private static Object[] pluginNames;
	private static Object[][] results;
	private static final int rows = 20;
	
	public static void summrize()
	{
		List<Plugin> plugins = AlgorithmFactory.getSelectedAlgorithms();
		
		pluginNames = new Object[plugins.size() + 1];
		
		pluginNames[0] = "Term";
		for(int i = 0; i < plugins.size(); i++)
			pluginNames[i + 1] = plugins.get(i).getName();
		
		results = new Object[rows][plugins.size() + 1];
		
		int rowCount = 0;
		rowCount = setPartitionKeys(rowCount, plugins);
	}
	
	private static int setPartitionKeys(int rowCount, List<Plugin> plugins)
	{
		Plugin aPlugin = plugins.get(0);
		HashMap<String, List<String>> tableKeyMap = aPlugin.getInstance().getPartitionKey();
		List<String> tableNames = new ArrayList<String>(tableKeyMap.keySet());
		
		for(int i = 0; i < tableNames.size(); i++)
		{
			results[rowCount + i][0] = tableNames.get(i);
			results[rowCount + i][1] = tableKeyMap.get(tableNames.get(i));
		}
		
		for(int i = 1; i < plugins.size(); i++)
		{
			aPlugin = plugins.get(i);
			tableKeyMap = aPlugin.getInstance().getPartitionKey();
			
			for(int j = 0; j < tableNames.size(); j++)
				results[rowCount + j][i + 1] = tableKeyMap.get(tableNames.get(j));
		}
		
		return rowCount;
	}
	
	public static Object[] getPluginNames()
	{
		return pluginNames;
	}
	public static Object[][] getSummaries()
	{
		return results;
	}
}