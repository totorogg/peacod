package com.emc.paradb.advisor.algorithm;

import java.util.ArrayList;
import java.util.List;

import com.emc.paradb.advisor.algorithm.finegrainedgraphparititioning.FineGrainedGraphPartitioning;
import com.emc.paradb.advisor.algorithm.hypergraphpartitioning.HyperGraphPartitioning;
import com.emc.paradb.advisor.algorithm.mintermGraph.MinTermGraph;
import com.emc.paradb.advisor.algorithm.mintermGraphFile.MinTermGraphFile;
import com.emc.paradb.advisor.algorithm.rewrite.RewriteHyperGraphPartitioning;
import com.emc.paradb.advisor.plugin.PlugInterface;
import com.emc.paradb.advisor.plugin.Plugin;

/**
 * This class maintains all algorithms' information
 * To add a bulit-in algorithm, you have to new a Plugin() and added in manually in this class.
 * To add a plug-in algorithm, you need to package the algorithm into a jar and then put in plugin directory 
 * of the project. A config.xml is also necessary to be added in the jar
 * 
 * @author Xin Pan
 *
 */
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
	private static Plugin MintermGraph = new Plugin();
	private static Plugin MintermGraphFile = new Plugin();
	private static Plugin FineGrainedGraph = new Plugin();
	private static Plugin HyperGraphPartitioning = new Plugin();
	private static Plugin RewriteHyperGraphPartitioning = new Plugin();
	//load the build-in algorithms
	static
	{
		countMaxRR.setInterface(new CountMaxRR());
		countMaxRR.setInfo("com.emc.paradb.advisor.algorithm.CountMaxRR",
							"getPartitionKey()", "getNode()",
							"In this scheme, those most frequently accessed columns are selected as the partition keys. The tables are partitioned in the round robin manner based on the partition key values.");//This scheme select partitioning keys that are most frequently accessed, and partition them in roundRobin manner.");
	
		schemaHash.setInterface(new SchemaHash());
		schemaHash.setInfo("com.emc.paradb.advisor.algorithm.SchemaHash",
							"getPartitionKey()", "getNode()",
							"This scheme selects partition keys based on the primary-foreign key relationship topology in the database schema. The primary key of the \"root table\" will become the main driving partition key.");
	
		PKHash.setInterface(new PKHash());
		PKHash.setInfo("com.emc.paradb.advisor.algorithm.PKHash",
							"getPartitionKey()", "getNode()",
							"This scheme selects primary keys of tables as the partition keys and hash-partition tables.");
	
		PKRange.setInterface(new PKRange());
		PKRange.setInfo("com.emc.paradb.advisor.algorithm.PKRange",
							"getPartitionKey()", "getNode()",
							"This scheme selects primary keys of tables as the partition keys and range-partition tables.");
	
		PKRoundRobin.setInterface(new PKRR());
		PKRoundRobin.setInfo("com.emc.paradb.advisor.algorithm.PKRoundRobin",
							"getPartitionKey()", "getNode()",
							"This scheme selects primary keys of tables as the partition keys and partition tables in the round-robin manner based on the partition key values.");
	
		AllReplicateHash.setInterface(new AllReplicateHash());
		AllReplicateHash.setInfo("com.emc.paradb.advisor.algorithm.AllReplicateHash",
							"getPartitionKey()", "getNode()",
							"The scheme replicates each table to all data nodes.");
		
		
		RangeGraph.setInterface(new RangeGraph());
		RangeGraph.setInfo("com.emc.paradb.advisor.algorithm.RangeGraph",
							"getPartitionKey()", "getNode()",
							"This scheme chooses the primary keys of tables as the partition keys and uses METIS to play graph partition");
		
		MintermGraph.setInterface(new MinTermGraph());
		MintermGraph.setInfo("com.emc.paradb.advisor.algorithm.MintermGraph",
							"getPartitionKey()", "getNode()",
							"This scheme analyses predicates in a workload and partitions tables by the midterm ranges.");
		
		MintermGraphFile.setInterface(new MinTermGraphFile());
		MintermGraphFile.setInfo("com.emc.paradb.advisor.algorithm.MintermGraphFile",
							"getPartitionKey()", "getNode()",
							"This scheme analyses predicates in a workload and partitions tables by the midterm ranges.");
		
		FineGrainedGraph.setInterface(new FineGrainedGraphPartitioning());
		FineGrainedGraph.setInfo("com.emc.paradb.advisor.algorithm.FineGrainedGraphPartitioning",
							"getPartitionKey()", "getNode()",
							"This scheme analyses predicates in a workload and partitions tables by the midterm ranges.");
		
		HyperGraphPartitioning.setInterface(new HyperGraphPartitioning());
		HyperGraphPartitioning.setInfo("com.emc.paradb.advisor.algorithm.HyperGraphPartitioning",
							"getPartitionKey()", "getNode()",
							"This scheme analyses predicates in a workload and partitions tables by the midterm ranges.");
		
		RewriteHyperGraphPartitioning.setInterface(new RewriteHyperGraphPartitioning());
		RewriteHyperGraphPartitioning.setInfo("com.emc.paradb.advisor.algorithm.RewriteHyperGraphPartitioning",
							"getPartitionKey()", "getNode()",
							"This scheme analyses predicates in a workload and partitions tables by the midterm ranges.");
		
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
	
	//build-in algorithms are added statically
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

		//algorithms.add(RangeGraph);
		algorithms.add(MintermGraph);
		algorithms.add(MintermGraphFile);
		algorithms.add(FineGrainedGraph);
		algorithms.add(HyperGraphPartitioning);
		algorithms.add(RewriteHyperGraphPartitioning);
		
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