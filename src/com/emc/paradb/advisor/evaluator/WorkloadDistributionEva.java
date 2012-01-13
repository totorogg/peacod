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
	private static int dist = 0;
	private static int nonDist = 0;
	private static int nodes = 0;
	private static Plugin aPlugin = null;
	private static HashMap<Integer, Integer> distCountMap = new HashMap<Integer, Integer>();
	private static List<Long> workloadDistList= new ArrayList<Long>();
	private static HashMap<String, String> tableKeyMap;
	
	public static void evaluate(Plugin aPlugin, int nodes)
	{
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
	protected static void analyzeTran(HashMap<Integer, Integer> visitMap, Transaction<Object> tran)
	{
		int distCount = 0;//how many nodes the transaction visits
		//partition key is not contained in all Sqls of the transaction
		for(int i = 0; i < nodes; i++)
		{
			visitMap.put(i, visitMap.get(i) + visitMap.get(-1));
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
		{
			dist++;
			for(Object stmt : tran)
			{
				SelectAnalysisInfo select = (SelectAnalysisInfo)stmt;
				System.out.println(select.getWhereKeys());
				System.out.println(select.getTables());
			}
		}
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
		String key = tableKeyMap.get(table);
		
		
		for(WhereKey whereKey : delete.getWhereKeys())
		{
			if(whereKey.getKeyName().equalsIgnoreCase(key))
			{
				updateWD(table, key, whereKey.getKeyValue().toString(), visitMap);
				return;
			}
		}
		visitMap.put(-1, visitMap.get(-1) + 1);
	}
	protected static void visitInsert(InsertAnalysisInfo insert, HashMap<Integer, Integer> visitMap)
	{
		String table = insert.getTable();
		String key = tableKeyMap.get(table);
		Map<String, Object> keyValueMap = insert.getKeyValueMap();
		
		for(String whereKey : keyValueMap.keySet())
		{
			if(whereKey.equalsIgnoreCase(key))
			{
				updateWD(table, key, keyValueMap.get(whereKey).toString(), visitMap);
				return;
			}
		}
		visitMap.put(-1, visitMap.get(-1) + 1);
	}
	//this funtion should be further improved.
	//now it is only aware of deleted old value, but not aware of the inserted new value
	protected static void visitUpdate(UpdateAnalysisInfo update, HashMap<Integer, Integer> visitMap)
	{
		String table = update.getTable();
		String key = tableKeyMap.get(table);
		for(WhereKey whereKey : update.getWhereKeys())
		{
			if(whereKey.getKeyName().equalsIgnoreCase(key))
			{
				updateWD(table, key, whereKey.getKeyValue().toString(), visitMap);
				return;
			}
		}
		visitMap.put(-1, visitMap.get(-1) + 1);
	}
	
	protected static void visitSelect(SelectAnalysisInfo select, HashMap<Integer, Integer> visitMap)
	{
		Set<String> tables  = select.getTables();
		boolean hit = false;
		
		for(String table : tables)
		{
			String key = tableKeyMap.get(table);
			if(key.equalsIgnoreCase("replicate"))
			{
				visitMap.put(-2, visitMap.get(-2) + 1);
				continue;
			}
			for(WhereKey whereKey : select.getWhereKeys())
			{
				if(whereKey.getKeyName().equalsIgnoreCase(key))
				{
					updateWD(table, key, whereKey.getKeyValue().toString(), visitMap);
					hit = true;
					break;
				}
			}
			if(!hit)
			{
				visitMap.put(-1, visitMap.get(-1) + 1);
				return;
			}
			hit = false;
		}
	}
	
	protected static void updateWD(String table,
								   String keyName, 
								   String value, 
								   HashMap<Integer, Integer> visitMap)
	{
		
		KeyValuePair kvPair = new KeyValuePair(table, keyName, value);
		int node = aPlugin.getInstance().getNode(kvPair);
		if(node == -1)//failed to match a node
		{
			System.out.println(String.format("cannot match a keyValue %s %s %s to its node", 
											 table, keyName, value));
			return;
		}
		if(visitMap.get(node) == null)
			visitMap.put(node, 1);
		else
			visitMap.put(node, visitMap.get(node) + 1);
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