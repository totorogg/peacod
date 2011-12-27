package com.emc.paradb.advisor.algorithm;

import java.util.ArrayList;
import java.util.List;

import com.emc.paradb.advisor.plugin.PlugInterface;
import com.emc.paradb.advisor.plugin.Plugin;


public class AlgorithmFactory
{
	private static List<Plugin> algorithms = new ArrayList<Plugin>();
	private static List<Plugin> selectedAlgorithms = new ArrayList<Plugin>();
	
	private static Plugin countMaxRR = new Plugin();
	//load the build-in algorithms
	static
	{
		countMaxRR.setInterface(new CountMaxRR());
		countMaxRR.setInfo("com.emc.paradb.advisor.algorithm.CountMaxRR",
							"getPartitionKey()", "getNode()",
							"this algorithm select the keys that are most frequently accessed, and partition them in roundRobin manner");
	}
	
	public static void loadBuildin()
	{
		algorithms.add(countMaxRR);
	}
	public static void ListAlgorithms()
	{
		for(Plugin aAlgorithm : algorithms)
			System.out.println(aAlgorithm.getID());
	}
	public static List<Plugin> getAlgorithms()
	{
		return algorithms;
	}
	
	public static void addAlgorithms(List<Plugin> newAlgorithms)
	{
		algorithms.addAll(newAlgorithms);
	}
	public static void removeAll()
	{
		algorithms = new ArrayList<Plugin>();
	}
	public static void addSelected(Plugin aAlgorithm)
	{
		selectedAlgorithms.add(aAlgorithm);
	}
	public static void removeSelected(Plugin aAlgorithm)
	{
		selectedAlgorithms.remove(aAlgorithm);
	}
	public static void ListSelectedAlgorithms()
	{
		for(Plugin aAlgorithm : selectedAlgorithms)
			System.out.println(aAlgorithm.getID());
	}
	public static List<Plugin> getSelectedAlgorithms()
	{
		return selectedAlgorithms;
	}
	

}