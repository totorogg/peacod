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

	private static Plugin AllReplicateHash = new Plugin();
	private static Plugin RangeGraph = new Plugin();
	private static Plugin Minterm = new Plugin();

	
	//load the build-in algorithms
	static
	{
		countMaxRR.setInterface(new CountMaxRR());
		countMaxRR.setInfo("com.emc.paradb.advisor.algorithm.CountMaxRR",
							"getPartitionKey()", "getNode()",
							"This scheme select the keys that are most frequently accessed, and partition them in roundRobin manner.");
	
		schemaHash.setInterface(new SchemaHash());
		schemaHash.setInfo("com.emc.paradb.advisor.algorithm.SchemaHash",
							"getPartitionKey()", "getNode()",
							"This algorithm select partition key based on database schema. primary key in \"root table\" is the basic partition key.");
	
		PKHash.setInterface(new PKHash());
		PKHash.setInfo("com.emc.paradb.advisor.algorithm.PKHash",
							"getPartitionKey()", "getNode()",
							"This algorithm select primary key as partition key and place values via hash function.");
	
		PKRange.setInterface(new PKRange());
		PKRange.setInfo("com.emc.paradb.advisor.algorithm.PKRange",
							"getPartitionKey()", "getNode()",
							"This algorithm select primary key as partition key and place values based on their range.");
	
		PKRoundRobin.setInterface(new PKRR());
		PKRoundRobin.setInfo("com.emc.paradb.advisor.algorithm.PKRoundRobin",
							"getPartitionKey()", "getNode()",
							"This algorithm select primary key as partition key and place values via round robin function.");
	
		AllReplicateHash.setInterface(new AllReplicateHash());
		AllReplicateHash.setInfo("com.emc.paradb.advisor.algorithm.AllReplicateHash",
							"getPartitionKey()", "getNode()",
							"The algorithm replicate table to all nodes.");
		
		
		RangeGraph.setInterface(new RangeGraph());
		RangeGraph.setInfo("com.emc.paradb.advisor.algorithm.RangeGraph",
							"getPartitionKey()", "getNode()",
							"choose primary key as partitionKey and use METIS to play graph partition");
		
		Minterm.setInterface(new MinTerm());
		Minterm.setInfo("com.emc.paradb.advisor.algorithm.Minterm",
							"getPartitionKey()", "getNode()",
							"This algorithm analyse predicates in a workload and partition by the midterm ranges.");
		
		//the following are not implemented
	/*	consistentHash.setInterface(new SchemaHash());
		consistentHash.setInfo("com.emc.paradb.advisor.algorithm.ConsistentHash",
							"getPartitionKey()", "getNode()",
							"This algorithm partition the key based on consistent hashing.");
	

		Dynamo.setInterface(new SchemaHash());
		Dynamo.setInfo("com.emc.paradb.advisor.algorithm.Dynamo",
							"getPartitionKey()", "getNode()",
							"According to dynamo paper.");
	

	
		semiSchema.setInterface(new SchemaHash());
		semiSchema.setInfo("com.emc.paradb.advisor.algorithm.SemiSchema",
							"getPartitionKey()", "getNode()",
							"This algorithm partition the root table based on predicates in workload and partition other tables by fk references.");*/
	}
	
	public static void loadBuildin()
	{
		algorithms.add(countMaxRR);
		algorithms.add(schemaHash);
		//algorithms.add(consistentHash);
		algorithms.add(PKHash);
		algorithms.add(PKRange);
		algorithms.add(PKRoundRobin);
		//algorithms.add(Dynamo);
		algorithms.add(AllReplicateHash);
		algorithms.add(RangeGraph);
		algorithms.add(Minterm);
		//algorithms.add(semiSchema);
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