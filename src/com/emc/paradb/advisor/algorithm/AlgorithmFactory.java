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
	private static Plugin schemaHash = new Plugin();
	private static Plugin PKHash = new Plugin();
	private static Plugin PKRange = new Plugin();
	private static Plugin PKRoundRobin = new Plugin();
	private static Plugin Dynamo = new Plugin();
	private static Plugin AllReplicate = new Plugin();
	private static Plugin AllMidterm = new Plugin();
	private static Plugin semiSchema = new Plugin();
	
	//load the build-in algorithms
	static
	{
		countMaxRR.setInterface(new CountMaxRR());
		countMaxRR.setInfo("com.emc.paradb.advisor.algorithm.CountMaxRR",
							"getPartitionKey()", "getNode()",
							"this algorithm select the keys that are most frequently accessed, and partition them in roundRobin manner");
	
		schemaHash.setInterface(new SchemaHash());
		schemaHash.setInfo("com.emc.paradb.advisor.algorithm.SchemaHash",
							"getPartitionKey()", "getPartitionKey()",
							"this algorithm select partition key based on database schema. primary key in \"root table\" is the basic partition key");
	
	
		PKHash.setInterface(new SchemaHash());
		PKHash.setInfo("com.emc.paradb.advisor.algorithm.PKHash",
							"getPartitionKey()", "getPartitionKey()",
							"this algorithm select primary key as partition key and place values via hash function");
	
		PKRange.setInterface(new SchemaHash());
		PKRange.setInfo("com.emc.paradb.advisor.algorithm.PKRange",
							"getPartitionKey()", "getPartitionKey()",
							"this algorithm select primary key as partition key and place values based on their range");
	
		PKRoundRobin.setInterface(new SchemaHash());
		PKRoundRobin.setInfo("com.emc.paradb.advisor.algorithm.PKRoundRobin",
							"getPartitionKey()", "getPartitionKey()",
							"this algorithm select primary key as partition key and place values via round robin function");
	
		Dynamo.setInterface(new SchemaHash());
		Dynamo.setInfo("com.emc.paradb.advisor.algorithm.Dynamo",
							"getPartitionKey()", "getPartitionKey()",
							"according to dynamo paper");
	
		AllReplicate.setInterface(new SchemaHash());
		AllReplicate.setInfo("com.emc.paradb.advisor.algorithm.AllReplicate",
							"getPartitionKey()", "getPartitionKey()",
							"the algorithm replicate table to all nodes");
	
		AllMidterm.setInterface(new SchemaHash());
		AllMidterm.setInfo("com.emc.paradb.advisor.algorithm.AllMidterm",
							"getPartitionKey()", "getPartitionKey()",
							"this algorithm analyse predicates in a workload and partition by the midterm ranges");
	
		semiSchema.setInterface(new SchemaHash());
		semiSchema.setInfo("com.emc.paradb.advisor.algorithm.SemiSchema",
							"getPartitionKey()", "getPartitionKey()",
							"this algorithm partition the root table based on predicates in workload and partition other tables by fk references");
	}
	
	public static void loadBuildin()
	{
		algorithms.add(countMaxRR);
		algorithms.add(schemaHash);
		algorithms.add(PKHash);
		algorithms.add(PKRange);
		algorithms.add(PKRoundRobin);
		algorithms.add(Dynamo);
		algorithms.add(AllReplicate);
		algorithms.add(AllMidterm);
		algorithms.add(semiSchema);
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