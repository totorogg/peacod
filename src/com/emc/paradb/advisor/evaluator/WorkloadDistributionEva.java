package com.emc.paradb.advisor.evaluator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.emc.paradb.advisor.controller.Controller;
import com.emc.paradb.advisor.data_loader.DBData;
import com.emc.paradb.advisor.data_loader.TableNode;
import com.emc.paradb.advisor.plugin.KeyValuePair;
import com.emc.paradb.advisor.plugin.Plugin;
import com.emc.paradb.advisor.workload_loader.DeleteAnalysisInfo;
import com.emc.paradb.advisor.workload_loader.InsertAnalysisInfo;
import com.emc.paradb.advisor.workload_loader.SelectAnalysisInfo;
import com.emc.paradb.advisor.workload_loader.Transaction;
import com.emc.paradb.advisor.workload_loader.UpdateAnalysisInfo;
import com.emc.paradb.advisor.workload_loader.WhereKey;
import com.emc.paradb.advisor.workload_loader.Workload;


public class WorkloadDistributionEva extends Evaluator
{
	private static int dist;
	private static int nonDist;
	private static int nodes;
	private static Plugin aPlugin = null;
	private static HashMap<Integer, Integer> distCountMap = null;
	private static List<Long> workloadDistList = null;
	private static HashMap<String, List<String>> tableKeyMap = null;
	
	public static void evaluate(Plugin aPlugin, int nodes)
	{
		dist = 0;
		nonDist = 0;
		distCountMap = new HashMap<Integer, Integer>();
		workloadDistList= new ArrayList<Long>();	
		WorkloadDistributionEva.aPlugin = aPlugin;
		Workload<Transaction<Object>> workload = Controller.getWorkload();
		tableKeyMap = aPlugin.getInstance().getPartitionKey();
		WorkloadDistributionEva.nodes = nodes;
		
		for(int i = 0; i < nodes; i++)
			workloadDistList.add(0L);
		
		for(Transaction<Object> tran : workload)
		{
			HashMap<Integer, Integer> visitMap = new HashMap<Integer, Integer>();
			for(int i = -2; i < nodes; i++)
				visitMap.put(i, 0);
			
			for(Object statement : tran)
			{
				if(statement instanceof SelectAnalysisInfo)
				{
					SelectAnalysisInfo select = (SelectAnalysisInfo)statement;
					visitSelect(select, visitMap);
				}
				else if(statement instanceof UpdateAnalysisInfo)
				{
					UpdateAnalysisInfo update = (UpdateAnalysisInfo)statement;
					visitUpdate(update, visitMap);
				}
				else if(statement instanceof InsertAnalysisInfo)
				{
					InsertAnalysisInfo insert = (InsertAnalysisInfo)statement;
					visitInsert(insert, visitMap);
				}
				else if(statement instanceof DeleteAnalysisInfo)
				{
					DeleteAnalysisInfo delete = (DeleteAnalysisInfo)statement;
					visitDelete(delete, visitMap);
				}
			}			
			analyzeTran(visitMap, tran);
		}
	}
	/**
	 * 
	 * @param visitMap
	 * @param tran
	 */
	protected static void analyzeTran(HashMap<Integer, Integer> visitMap, Transaction<Object> tran)
	{
		int distCount = 0;//how many nodes the transaction visits
		//partition key is not contained in all Sqls of the transaction
		for(int i = 0; i < nodes; i++)
		{
			if(visitMap.get(-1) > 0)
				visitMap.put(i, visitMap.get(i) + 1);
			workloadDistList.set(i, workloadDistList.get(i) + visitMap.get(i));
			if(visitMap.get(i) != 0)
				distCount++;
		}
		//if no table other than the replicated table is visited, we fetch the first node for the 
		//replicated data
		if(distCount == 0 && visitMap.get(-2) != 0)
		{
			distCount++;
			workloadDistList.set(0, workloadDistList.get(0) + visitMap.get(-2));
		}
		
		if(distCount > 1)
			dist++;
		else
			nonDist++;

		if(distCountMap.get(distCount) == null)
			distCountMap.put(distCount, 1);
		else
			distCountMap.put(distCount, distCountMap.get(distCount) + 1);
		
	}
	
	protected static void visitDelete(DeleteAnalysisInfo delete, HashMap<Integer, Integer> visitMap)
	{
		String table = delete.getTable();
		List<String> keys = tableKeyMap.get(table);	
		List<KeyValuePair> kvPairs = new ArrayList<KeyValuePair>();

		for(WhereKey whereKey : delete.getWhereKeys())
		{
			if(keys.contains(whereKey.getKeyName()) && whereKey.getKeyValue() != null)
			{
				KeyValuePair kvPair = new KeyValuePair(table, whereKey.getKeyName(), whereKey.getKeyValue());
				kvPair.setOpera("delete");
				kvPairs.add(kvPair);
			}
		}
		if(kvPairs.size() > 0)
			updateWD(table, kvPairs, visitMap);
		else
			visitMap.put(-1, visitMap.get(-1) + 1);
	}
	
	protected static void visitInsert(InsertAnalysisInfo insert, HashMap<Integer, Integer> visitMap)
	{
		String table = insert.getTable();
		List<String> keys = tableKeyMap.get(table);
		Map<String, String> keyValueMap = insert.getKeyValueMap();
		List<KeyValuePair> kvPairs = new ArrayList<KeyValuePair>();
		
		for(String whereKey : keyValueMap.keySet())
		{
			if(keys.contains(whereKey) && keyValueMap.get(whereKey) != null)
			{
				KeyValuePair kvPair = new KeyValuePair(table, whereKey, keyValueMap.get(whereKey));
				kvPair.setOpera("insert");
				kvPairs.add(kvPair);
			}
		}
		if(kvPairs.size() > 0)
			updateWD(table, kvPairs, visitMap);
		else
			visitMap.put(-1, visitMap.get(-1) + 1);
	}
	//this funtion should be further improved.
	//now it is only aware of deleted old value, but not aware of the inserted new value
	protected static void visitUpdate(UpdateAnalysisInfo update, HashMap<Integer, Integer> visitMap)
	{
		String table = update.getTable();
		List<String> keys = tableKeyMap.get(table);
		List<KeyValuePair> kvPairs = new ArrayList<KeyValuePair>();

		for(WhereKey whereKey : update.getWhereKeys())
		{
			if(keys.contains(whereKey.getKeyName()) && whereKey.getKeyValue() != null)
			{
				KeyValuePair kvPair = new KeyValuePair(table, whereKey.getKeyName(), whereKey.getKeyValue());
				kvPair.setOpera("delete");
				kvPairs.add(kvPair);
			}
		}
		
		if(kvPairs.size() > 0)
			updateWD(table, kvPairs, visitMap);
		else
			visitMap.put(-1, visitMap.get(-1) + 1);
	}
	
	protected static void visitSelect(SelectAnalysisInfo select, HashMap<Integer, Integer> visitMap)
	{
		Set<String> tables  = select.getTables();
		boolean hit = false;
		
		for(String table : tables)
		{
			List<String> keys = tableKeyMap.get(table);
			List<KeyValuePair> kvPairs = new ArrayList<KeyValuePair>();
			
			if(keys.get(0).equalsIgnoreCase("replicate"))
			{
				KeyValuePair kvPair = new KeyValuePair(table, null, "replicate");
				kvPair.setOpera("select");
				kvPairs.add(kvPair);
				hit = true;
			}
			
			for(WhereKey whereKey : select.getWhereKeys())
			{
				if(keys.contains(whereKey.getKeyName()) && whereKey.getKeyValue() != null)
				{
					KeyValuePair kvPair = new KeyValuePair(table, whereKey.getKeyName(), whereKey.getKeyValue());
					kvPair.setOpera("select");
					kvPairs.add(kvPair);
					hit = true;
				}
			}
			if(!hit)
			{
				visitMap.put(-1, visitMap.get(-1) + 1);
				return;
			}
			hit = false;
			
			updateWD(table, kvPairs, visitMap);
		}
	}
	
	protected static void updateWD(String table,
								   List<KeyValuePair> kvPairs, 
								   HashMap<Integer, Integer> visitMap)
	{
		
		List<Integer> nodeList = aPlugin.getInstance().getNode(kvPairs);
		if(nodeList.get(0) == -1)//failed to match a node
		{
			System.out.println(String.format("cannot match a keyValuePairs %s to its node", kvPairs));
			visitMap.put(-1, visitMap.get(-1)+1);
			return;
		}
		for(Integer node : nodeList)
		{
			if (visitMap.get(node) == null)
				visitMap.put(node, 1);
			else
				visitMap.put(node, visitMap.get(node) + 1);
		}
	}
	
	public static List<Long> getWorkloadDistribution()
	{
		return workloadDistList;
	}
	public static HashMap<Integer, Integer> getDistCountMap()
	{
		return distCountMap;
	}
	public static int getDistCount()
	{
		return dist;
	}
	public static int getNonDistCount()
	{
		return nonDist;
	}
}