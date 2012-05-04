package com.emc.paradb.advisor.evaluator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.emc.paradb.advisor.algorithm.AlgorithmFactory;
import com.emc.paradb.advisor.data_loader.DataLoader;
import com.emc.paradb.advisor.data_loader.TableNode;
import com.emc.paradb.advisor.plugin.Plugin;

/**
 * summarize the evaluate information of all algorithms
 * this class retrieve all partition information
 * all partition results are already stored in
 * plugin classes.
 * Hence, the work of this class just retrieve
 * data from plugins and aggregate them.
 * 
 * @author Xin Pan
 *
 */
public class Summarize
{
	private static Object[] pluginNames;
	private static Object[][] tableKeyMapResults;
	private static Object[][] workloadDistResults;
	private static final int rows = 10;
	
	public static void summrize()
	{
		List<Plugin> plugins = AlgorithmFactory.getSelectedAlgorithms();
		
		pluginNames = new Object[plugins.size() + 1];
		
		//pluginNames[0] = "Term";
		pluginNames[0] = " ";
		for(int i = 0; i < plugins.size(); i++)
			pluginNames[i + 1] = plugins.get(i).getName();
		
		
		tableKeyMapResults = new Object[rows][plugins.size() + 1];
		setEvaluateResult(tableKeyMapResults, plugins);
		
		workloadDistResults = new Object[rows][plugins.size() + 1];
		setPartitionKeys(workloadDistResults, plugins);
		
	}
	
	private static int setEvaluateResult(Object[][] results, List<Plugin> plugins)
	{
		int rowCount = 0;
		
		results[rowCount][0] = "#Dist-Xacts";
		for(int i = 0; i < plugins.size(); i++)
			results[rowCount][i+1] = String.valueOf(plugins.get(i).getDist());
		
		results[++rowCount][0] = "#Nondist-Xacts";
		for(int i = 0; i < plugins.size(); i++)
			results[rowCount][i+1] = String.valueOf(plugins.get(i).getNonDist());
		
		
		results[++rowCount][0] = "#SQLs per Node";
		for(int i = 0; i < plugins.size(); i++)
		{
			List<Long> workloadList = plugins.get(i).getWorkloadDistribution();
			long sum = 0;
			for(int j = 0; j < workloadList.size(); j++)
				sum += workloadList.get(j);
			results[rowCount][i+1] = String.valueOf(sum / workloadList.size());
		}
		
		results[++rowCount][0] = "#Nodes per Xact";
		for(int i = 0; i < plugins.size(); i++)
		{
			HashMap<Integer, Integer> nodeAccess = plugins.get(i).getNodeAccess();
			int sum = 0;
			int sumXact = 0;
			for(Integer nodeCount : nodeAccess.keySet())
			{
				sum += nodeCount * nodeAccess.get(nodeCount);
				sumXact += nodeAccess.get(nodeCount);
			}
			results[rowCount][i+1] = String.valueOf( sum / sumXact );
		}
		
		return ++rowCount;
	}
	
	private static int setPartitionKeys(Object[][] results, List<Plugin> plugins)
	{
		int rowCount = 0;
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
		
		return rowCount + tableNames.size();
	}
	
	public static Object[] getPluginNames()
	{
		return pluginNames;
	}
	public static Object[][] getTableKeyMap()
	{
		return tableKeyMapResults;
	}
	public static Object[][] getWorkloadDist()
	{
		return workloadDistResults;
	}
}