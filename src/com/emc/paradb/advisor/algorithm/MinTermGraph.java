package com.emc.paradb.advisor.algorithm;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.Vector;

import com.emc.paradb.advisor.data_loader.DBData;
import com.emc.paradb.advisor.data_loader.TableAttributes;
import com.emc.paradb.advisor.data_loader.TableNode;
import com.emc.paradb.advisor.plugin.KeyValuePair;
import com.emc.paradb.advisor.plugin.PlugInterface;
import com.emc.paradb.advisor.utils.QueryPrepare;
import com.emc.paradb.advisor.workload_loader.DeleteAnalysisInfo;
import com.emc.paradb.advisor.workload_loader.InsertAnalysisInfo;
import com.emc.paradb.advisor.workload_loader.SelectAnalysisInfo;
import com.emc.paradb.advisor.workload_loader.Transaction;
import com.emc.paradb.advisor.workload_loader.UpdateAnalysisInfo;
import com.emc.paradb.advisor.workload_loader.WhereKey;
import com.emc.paradb.advisor.workload_loader.WhereKey.Range;
import com.emc.paradb.advisor.workload_loader.Workload;

/**
 * This algorithm extract all predicates from workload 
 * and construct minTerms. Each minTerm is regarded as 
 * a graph node. if two nodes are visited in a transaction,
 * an edge is added between two nodes
 * 
 * after graph construction, we partition it with METIS
 * 
 * 
 * @author Xin Pan
 *
 */
public class MinTermGraph implements PlugInterface
{
	Connection conn = null;
	Workload<Transaction<Object>> workload = null;
	DBData dbData = null;
	int nodes = 0;
	int partitionCount = 0;
	
	HashMap<String, TablePartition> tablePartitions = null;
	List<String> tableList = null;
	HashMap<String, Integer> tableStartPos = null;
	HashMap<String, Integer> tableEndPos = null;
	HashMap<String, List<String>> tableKeyMap = null;
	List<MinTerm> minTermList = null;
	
	@Override
	public boolean accept(Connection conn, Workload<Transaction<Object>> workload,
			DBData dbData, int nodes) 
	{
		this.conn = conn;
		this.workload = workload;
		this.dbData = dbData;
		this.nodes = nodes;
		partitionCount = 0;
		
		tableKeyMap = new HashMap<String, List<String>>();
		tablePartitions = new HashMap<String, TablePartition>();
		
		try
		{
			setPartition();
			workload2Graph();
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		return true;
	}

	
	
	private void workload2Graph() throws Exception
	{
		//construct minterm list
		serialize();
		
		//add edges among minterms
		for(Transaction<Object> aTran : workload)
			explainTran(aTran);
		
		//combine unvisited minterms
		combine();
		
		
		for(int i = 0; i < tableList.size(); i++)
		{
			String tableName = tableList.get(i);
			TablePartition aTablePartition = tablePartitions.get(tableName);
			System.out.print(aTablePartition.getName() + ": ");
			List<String> keyList = aTablePartition.getKeyList();
			for(int j = 0; j < keyList.size(); j++)
				System.out.print(aTablePartition.getKeyPartition(keyList.get(j)).getName() + "\t");
		}
		
		//partition the minterm graph
		partitionGraph();
		
		for(int i = 0; i < tableList.size(); i++)
		{
			List<String> keyList = new ArrayList<String>();
			keyList.add("undefined");
			tableKeyMap.put(tableList.get(i), keyList);
		}
		
		List<String> keyList = new ArrayList<String>();
		keyList.add("replicate");
		tableKeyMap.put("item", keyList);
		
		
		//display the placement strategy, for validation
		BufferedWriter out = new BufferedWriter(new FileWriter("placement"));
		for(int i = 0; i < nodes; i++)
		{
			for(int j = 0; j < minTermList.size(); j++)
			{
				if(minTermList.get(j).getNode() == i)
					out.write(minTermList.get(j).toString() + "&&");
			}
			out.write("\n*******************************\n");
		}
		out.close();
	}
	
	private void partitionGraph() throws Exception
	{
		
		int edgeCount = prepareForMETIS();
		
		if(edgeCount % 2 != 0)
			System.err.println("Error edge count");
		edgeCount /= 2;
		
		
		String graphFile = "minTermGraph";		
		FileWriter fstream = new FileWriter(graphFile);
		BufferedWriter out = new BufferedWriter(fstream);
		
		out.write(minTermList.size() + " " + edgeCount + " 011\n");
		
		for(int i = 0; i < minTermList.size(); i++)
		{
			if(i > 10)
				out.write(minTermList.get(i).getEstimatedSize() + " ");
			else
				out.write(1000000 + " ");
			HashMap<Integer, Integer> neighbourMap = minTermList.get(i).getNeighbour();
			for (Integer neighbourID : neighbourMap.keySet())
				out.write(neighbourID + " " + neighbourMap.get(neighbourID) + " ");
			out.write("\n");
		}
		out.close();
		
		Process p = Runtime.getRuntime().exec("gpmetis " + graphFile + " " + nodes);
		p.waitFor();
	
		
		FileReader instream = new FileReader(graphFile + ".part." + nodes);
		BufferedReader in = new BufferedReader(instream);
		
		for(int i = 0; i < minTermList.size(); i++)
		{
			int node = Integer.valueOf(in.readLine());
			minTermList.get(i).setNode(node);
		}
		in.close();
	}
	
	private int prepareForMETIS()
	{
		int edgeCount = 0;
		HashMap<Integer, Integer> posMap = new HashMap<Integer, Integer>();
		
		for(int i = 0; i < minTermList.size(); i++)
		{
			posMap.put(minTermList.get(i).getPos(), i+1);
			minTermList.get(i).setPos(i+1);
		}
		for(int i = 0; i < minTermList.size(); i++)
		{
			edgeCount += minTermList.get(i).renewPos(posMap);
		}
		return edgeCount;
	}
	
	
	
	private void combine()
	{
		int count = 0;
		int rmCount = 0;
		
		int lastValidMT = 0;
		int validPos = 0;
		for(int i = 1; i < minTermList.size(); i++)
		{
			if(minTermList.get(i).getNeibourSize() != 0)
			{
				lastValidMT = minTermList.get(i).getPos();
				validPos = i;
				break;
			}
		}
		
		for(int i = 0; i < tableList.size(); i++)
		{
			String tableName = tableList.get(i);
			tableStartPos.put(tableName, tableStartPos.get(tableName) - rmCount);
			int s = tableStartPos.get(tableName);
			int e = tableEndPos.get(tableName) - rmCount;
			int cmp = e - s;
			
			for(int j = 0; j < cmp; j++)
			{
				MinTerm aMinTerm = minTermList.get(s);
				if(aMinTerm.getNeibourSize() == 0)
				{
					if (s + 1 >= minTermList.size() || !minTermList.get(s + 1).combinePre(aMinTerm))
					{
						if (s - 1 < 0 || !minTermList.get(s - 1).combineNext(aMinTerm)) 
						{
							count++;
							aMinTerm.addConnect(lastValidMT);
							minTermList.get(validPos).addConnect(aMinTerm.getPos());
							System.err.println("Error: Unable To Combine: " + s);
							s++;
							/*
							minTermList.remove(s);
							rmCount++;*/
							continue;
						}
					}
					minTermList.remove(s);
					rmCount++;
				}
				else
				{
					lastValidMT = minTermList.get(s).getPos();
					validPos = s;
					s++;
				}
			}
			tableEndPos.put(tableName, tableEndPos.get(tableName) - rmCount);
		}
		System.out.println(count + "\t" + rmCount);

	}
	
	/*
	 * translate predicates into minTerms lists.
	 * Each table has its start position and end position in the list.
	 */
	private void serialize()
	{
		tableList = new ArrayList<String>();
		tableStartPos = new HashMap<String, Integer>();
		tableEndPos = new HashMap<String, Integer>();
		minTermList = new ArrayList<MinTerm>();
		
		for(TablePartition aTablePartition : tablePartitions.values())
		{
			tableList.add(aTablePartition.getName());
			
			tableStartPos.put(aTablePartition.getName(), partitionCount);
			partitionCount += aTablePartition.serialize(minTermList);
			tableEndPos.put(aTablePartition.getName(), partitionCount);
		}
	}
	
	private void extractTran(Transaction<Object> aTran)
	{
		for(Object statement : aTran)
		{
			if(statement instanceof SelectAnalysisInfo)
			{
				SelectAnalysisInfo select = (SelectAnalysisInfo)statement;
				
				for(WhereKey key : select.getWhereKeys())
				{						
					String tableName = key.getTableName();
					TablePartition aTablePartition = tablePartitions.get(tableName);
					if(aTablePartition == null)
						continue;
					
					String aKey = key.getKeyName();
					KeyPartition aKeyPartition = aTablePartition.getKeyPartition(aKey);
					if(aKeyPartition == null)
						continue;
					
					Predicate aPredicate = extractPredicate(aKey, key.getKeyValue(), key.getRange());
					if(aPredicate != null)
						aKeyPartition.addPredicate(aPredicate);
				}
			}
			else if(statement instanceof UpdateAnalysisInfo)
			{
				UpdateAnalysisInfo update = (UpdateAnalysisInfo)statement;
				for(WhereKey key : update.getWhereKeys())
				{
					String tableName = key.getTableName();
					TablePartition aTablePartition = tablePartitions.get(tableName);
					if(aTablePartition == null)
						continue;
					
					String aKey = key.getKeyName();
					KeyPartition aKeyPartition = aTablePartition.getKeyPartition(aKey);
					if(aKeyPartition == null)
						continue;
					
					Predicate aPredicate = extractPredicate(aKey, key.getKeyValue(), key.getRange());
					if(aPredicate != null)
						aKeyPartition.addPredicate(aPredicate);
				}
			}
			else if(statement instanceof InsertAnalysisInfo)
			{
				InsertAnalysisInfo insert = (InsertAnalysisInfo)statement;
				
				for(String key : insert.getKeyValueMap().keySet())
				{
					String tableName = insert.getTable();
					TablePartition aTablePartition = tablePartitions.get(tableName);
					if(aTablePartition == null)
						continue;
					
					KeyPartition aKeyPartition = aTablePartition.getKeyPartition(key);
					if(aKeyPartition == null)
						continue;
					
					Predicate aPredicate = extractPredicate(key, insert.getKeyValueMap().get(key), Range.EQUAL);
					if(aPredicate != null)
						aKeyPartition.addPredicate(aPredicate);
				}
			}
			else if(statement instanceof DeleteAnalysisInfo)
			{
				DeleteAnalysisInfo delete = (DeleteAnalysisInfo)statement;
				for(WhereKey key : delete.getWhereKeys())
				{
					String tableName = key.getTableName();
					TablePartition aTablePartition = tablePartitions.get(tableName);
					if(aTablePartition == null)
						continue;
					
					String aKey = key.getKeyName();
					KeyPartition aKeyPartition = aTablePartition.getKeyPartition(aKey);
					if(aKeyPartition == null)
						continue;
					
					Predicate aPredicate = extractPredicate(aKey, key.getKeyValue(), key.getRange());
					if(aPredicate != null)
						aKeyPartition.addPredicate(aPredicate);
				}
			}
		}
	}
	
	private void setPartition()
	{
		preparePredicates();
		
		for(Transaction<Object> aTran : workload)
			extractTran(aTran);	
		
		eliminateKey();
		
		try 
		{
			BufferedWriter out = new BufferedWriter(new FileWriter("minterm"));
			listPartitions(out);
			out.close();
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	private void eliminateKey()
	{
		for(TablePartition aTablePartition : tablePartitions.values())
		{
			aTablePartition.eliminateKey();
		}
	}
	
	private void listPartitions(BufferedWriter out) throws IOException
	{
		for(TablePartition aTablePartition : tablePartitions.values())
		{
			out.write(aTablePartition.getName() + "\t");
			aTablePartition.listPartitions(out);
		}
	}
	
	private void explainTran(Transaction<Object> aTran)
	{
		HashMap<String, List<KeyValuePair>> tableKeyValueMap = new HashMap<String, List<KeyValuePair>>();
		
		for(Object statement : aTran)
		{
			if(statement instanceof SelectAnalysisInfo)
			{
				SelectAnalysisInfo select = (SelectAnalysisInfo)statement;
				
				for(WhereKey key : select.getWhereKeys())
				{						
					String tableName = key.getTableName();
					if(tablePartitions.get(tableName) == null ||
							tablePartitions.get(tableName).getKeyPartition(key.getKeyName()) == null)
						continue;
					
					if(tableKeyValueMap.get(tableName) == null)
					{
						List<KeyValuePair> aKeyValueList = new ArrayList<KeyValuePair>();
						KeyValuePair aKVPair = new KeyValuePair(key.getKeyName(), key.getKeyValue());
						aKeyValueList.add(aKVPair);
						tableKeyValueMap.put(tableName, aKeyValueList);
					}
					else
					{
						List<KeyValuePair> aKeyValueList = tableKeyValueMap.get(tableName);
						KeyValuePair aKVPair = new KeyValuePair(key.getKeyName(), key.getKeyValue());
						aKeyValueList.add(aKVPair);
						tableKeyValueMap.put(tableName, aKeyValueList);
					}
					
				}
			}
			else if(statement instanceof UpdateAnalysisInfo)
			{
				UpdateAnalysisInfo update = (UpdateAnalysisInfo)statement;
				for(WhereKey key : update.getWhereKeys())
				{
					String tableName = key.getTableName();
					if(tablePartitions.get(tableName) == null || 
							tablePartitions.get(tableName).getKeyPartition(key.getKeyName()) == null)
						continue;
					
					if(tableKeyValueMap.get(tableName) == null)
					{
						List<KeyValuePair> aKeyValueList = new ArrayList<KeyValuePair>();
						KeyValuePair aKVPair = new KeyValuePair(key.getKeyName(), key.getKeyValue());
						aKeyValueList.add(aKVPair);
						tableKeyValueMap.put(tableName, aKeyValueList);
					}
					else
					{
						List<KeyValuePair> aKeyValueList = tableKeyValueMap.get(tableName);
						KeyValuePair aKVPair = new KeyValuePair(key.getKeyName(), key.getKeyValue());
						aKeyValueList.add(aKVPair);
						tableKeyValueMap.put(tableName, aKeyValueList);
					}
					
				}
			}
			else if(statement instanceof InsertAnalysisInfo)
			{
				InsertAnalysisInfo insert = (InsertAnalysisInfo)statement;
				
				for(String key : insert.getKeyValueMap().keySet())
				{
					String tableName = insert.getTable();
					if(tablePartitions.get(tableName) == null || 
							tablePartitions.get(tableName).getKeyPartition(key) == null)
						continue;
					
					if(tableKeyValueMap.get(tableName) == null)
					{
						List<KeyValuePair> aKeyValueList = new ArrayList<KeyValuePair>();
						KeyValuePair aKVPair = new KeyValuePair(key, insert.getKeyValueMap().get(key));
						aKeyValueList.add(aKVPair);
						tableKeyValueMap.put(tableName, aKeyValueList);
					}
					else
					{
						List<KeyValuePair> aKeyValueList = tableKeyValueMap.get(tableName);
						KeyValuePair aKVPair = new KeyValuePair(key, insert.getKeyValueMap().get(key));
						aKeyValueList.add(aKVPair);
						tableKeyValueMap.put(tableName, aKeyValueList);
					}
				}
			}
			else if(statement instanceof DeleteAnalysisInfo)
			{
				DeleteAnalysisInfo delete = (DeleteAnalysisInfo)statement;
				for(WhereKey key : delete.getWhereKeys())
				{
					String tableName = key.getTableName();
					if(tablePartitions.get(tableName) == null || 
							tablePartitions.get(tableName).getKeyPartition(key.getKeyName()) == null)
						continue;
					
					if(tableKeyValueMap.get(tableName) == null)
					{
						List<KeyValuePair> aKeyValueList = new ArrayList<KeyValuePair>();
						KeyValuePair aKVPair = new KeyValuePair(key.getKeyName(), key.getKeyValue());
						aKeyValueList.add(aKVPair);
						tableKeyValueMap.put(tableName, aKeyValueList);
					}
					else
					{
						List<KeyValuePair> aKeyValueList = tableKeyValueMap.get(tableName);
						KeyValuePair aKVPair = new KeyValuePair(key.getKeyName(), key.getKeyValue());
						aKeyValueList.add(aKVPair);
						tableKeyValueMap.put(tableName, aKeyValueList);
					}
					
				}
			}
		}
		explainTableKeyValueMap(tableKeyValueMap);
	}
	
	private void explainTableKeyValueMap(HashMap<String, List<KeyValuePair>> tableKeyValueMap)
	{
		Set<Integer> visitNodes = new HashSet<Integer>();
		
		for(String tableName : tableKeyValueMap.keySet())
		{
			int startPos = tableStartPos.get(tableName);
			int endPos = tableEndPos.get(tableName);
			List<KeyValuePair> kvList = tableKeyValueMap.get(tableName);
			
			List<Predicate> searchMT = initSearchMinTerm(tableName, kvList);
			if(searchMT == null)
				continue;
			for(int i = startPos; i < endPos; i++)
			{
				if(minTermList.get(i).match(searchMT))
					visitNodes.add(i);
			}
		}
		
		for(Integer aNode : visitNodes)
			for(Integer aVisitNode : visitNodes)
				if(! aVisitNode.equals(aNode))
					minTermList.get(aNode).addConnect(minTermList.get(aVisitNode).getPos());
	}
	
	private List<Predicate> initSearchMinTerm(String tableName, List<KeyValuePair> kvList)
	{
		List<String> keyList = tablePartitions.get(tableName).getKeyList();
		List<Predicate> predicates = new ArrayList<Predicate>();
		int count = 0;

		
		for(int i = 0; i < keyList.size(); i++)
		{
			boolean found = false;
			for(int j = 0; j < kvList.size(); j++)
			{
				if(keyList.get(i).equals(kvList.get(j).getKey()))
				{
					KeyValuePair kvPair = kvList.get(j);
					if(kvPair.getValue() == null)
						return null;
					int min = Integer.valueOf(kvPair.getValue());
					int max = min+1;
					
					predicates.add(new Predicate(min, max));//note that we consider equal only
					found = true;
					break;
				}
			}
			if(!found)
			{
				predicates.add(new Predicate(Integer.MAX_VALUE, Integer.MIN_VALUE));
				count++;
			}
		}
		if(count == keyList.size())
			return null;
		else
			return predicates;
	}
	
	public Predicate extractPredicate(String keyName, String keyValue, Range range)
	{
		if(keyValue == null)
		{
			return null;
		}
		Predicate newPredicate = new Predicate();
	
		int value = 0;
		try
		{
			value = Integer.valueOf(keyValue);
		}catch(NumberFormatException e)
		{
			return null;
		}
		
		if(range == Range.EQUAL)
		{
			newPredicate.setMax(value + 1);
			newPredicate.setMin(value);
		}
		else if(range == Range.LARGEEQL)
		{
			newPredicate.setMin(value);
		}
		else if(range == Range.LARGER)
		{
			newPredicate.setMin(value + 1);
		}
		else if(range == Range.SMALLEQL)
		{
			newPredicate.setMax(value + 1);
		}
		else if(range == Range.SMALLER)
		{
			newPredicate.setMax(value);
		}
		else
		{
			try {
				throw new Exception("Unsupported Range");
				} catch (Exception e) {
				// TODO Auto-generated catch block
				System.err.println(e.getMessage());
				e.printStackTrace();
				return null;
			}
		}
		
		return newPredicate;
	}
	
	public void preparePredicates()
	{
		HashMap<String, TableNode> tables = dbData.getMetaData();
		for(TableNode aTableNode : tables.values())
		{
			if(aTableNode.getName().equals("item"))
				continue;
			
			TablePartition aTablePartition = new TablePartition(aTableNode, conn);
			tablePartitions.put(aTableNode.getName(), aTablePartition);
		}
	}
	
	@Override
	public HashMap<String, List<String>> getPartitionKey() {
		// TODO Auto-generated method stub
		return tableKeyMap;
	}

	@Override
	public List<Integer> getNode(List<KeyValuePair> kvPairs) 
	{
		// TODO Auto-generated method stub
		
		List<Integer> nodeList = new ArrayList<Integer>();
		
		
		if(kvPairs.get(0).getValue().equalsIgnoreCase("replicate"))
		{
			nodeList.add(-2);
			return nodeList;
		}
		
		else if(kvPairs.get(0).getKey().equals("undefined"))
		{
			for(int i = 0; i < nodes; i++)
				nodeList.add(0);
			return nodeList;
		}
		
		String tableName = kvPairs.get(0).getTable();
		
		List<Predicate> searchMT = initSearchMinTerm(tableName, kvPairs);
		if(searchMT == null)
		{
			for(int i = 0; i < nodes; i++)
				nodeList.add(-1);
			return nodeList;
		}
		
		int startPos = tableStartPos.get(tableName);
		int endPos = tableEndPos.get(tableName);
		for(int i = startPos; i < endPos; i++)
		{
			if(minTermList.get(i).match(searchMT))
			{
				MinTerm matchMT = minTermList.get(i);
				nodeList.add(matchMT.getNode());
			}
		}
		if(nodeList.size() == 0)
			nodeList.add(-1);
		
		return nodeList;
	}

	@Override
	public List<String[]> getSetting() {
		// TODO Auto-generated method stub
		return null;
	}
}

class TablePartition
{
	private String tableName;
	private HashMap<String, KeyPartition> keyPartitions = new HashMap<String, KeyPartition>();
	HashMap<String, Integer> keyVisitMap = new HashMap<String, Integer>();
	HashMap<String, Integer> keyPartitionCount = new HashMap<String, Integer>();
	
	int partitionCount = 0;
	List<String> keyList = null;
	
	public TablePartition(String tableName)
	{
		this.tableName = tableName;
	}
	
	public TablePartition(TableNode aTableNode, Connection conn)
	{
		tableName = aTableNode.getName();
		
		Vector<TableAttributes> attrs = aTableNode.getAttrVector();

		for(TableAttributes aAttr : attrs)
		{
			String aKey = aAttr.getName();
			try
			{
				Statement stmt = conn.createStatement();
				ResultSet minResult = stmt.executeQuery("select min(" + aKey +") " +
														"from " + QueryPrepare.prepare(tableName) + ";");
				
				int type = minResult.getMetaData().getColumnType(1);
				if(type != Types.INTEGER)
					continue;
				
				minResult.next();
				//int min = minResult.getInt(1);
				int min = Integer.MIN_VALUE / 2;
				
				ResultSet maxResult = stmt.executeQuery("select max(" + aKey +") " +
														"from " + QueryPrepare.prepare(tableName) + ";");
				maxResult.next();
				//int max = maxResult.getInt(1); 
				int max = Integer.MAX_VALUE / 2;
				
				KeyPartition aKeyPartition = new KeyPartition(aKey, min, max + 1);
				keyPartitions.put(aKey, aKeyPartition);
			}
			catch(SQLException e)
			{
				System.err.println(e.getMessage());
				e.printStackTrace();
			}
		}
	}
	
	public List<String> getKeyList()
	{
		return keyList;
	}
	
	public int serialize(List<MinTerm> minTermList)
	{
		final int startIndex = minTermList.size();
		keyList = new ArrayList<String>();
		
		for(KeyPartition aKeyPartition : keyPartitions.values())
		{
			keyList.add(aKeyPartition.getName());
			keyPartitionCount.put(aKeyPartition.getName(), aKeyPartition.getPartitionCount());
			
			if(minTermList.size() == startIndex)
			{
				List<Predicate> predicates = aKeyPartition.getPredicates();
				for(int i = 0; i < predicates.size(); i++)
				{
					MinTerm aMinTerm = new MinTerm(predicates.get(i));
					aMinTerm.setPos( ++partitionCount + startIndex);
					minTermList.add(aMinTerm);
				}
			}
			else
			{
				partitionCount = 0;
				List<Predicate> predicates = aKeyPartition.getPredicates();
				int tempSize = minTermList.size();
				for(int i = startIndex; i < tempSize; i++)
				{
					MinTerm oldMinTerm = minTermList.get(startIndex);
					minTermList.remove(startIndex);
					for(int j = 0; j < predicates.size(); j++)
					{
						MinTerm newMinTerm = new MinTerm(oldMinTerm, predicates.get(j));
						newMinTerm.setPos( ++partitionCount + startIndex);
						minTermList.add(newMinTerm);
					}
					oldMinTerm = null;
				}
			}
		}
		
		return partitionCount;
	}
	
	public Integer getPartitionCount()
	{
		int count = 0;
		for(int i = 0; i < keyList.size(); i++)
		{
			String keyName = keyList.get(i);
			KeyPartition aKeyPartition = keyPartitions.get(keyName);
			count += aKeyPartition.getPartitionCount();
		}
		return count;
	}
	
	public void listPartitions(BufferedWriter out) throws IOException
	{
		for(KeyPartition aKeyPartition : keyPartitions.values())
		{
			out.write(aKeyPartition.getName() + "\n");
			aKeyPartition.listPartitions(out);
		}
		System.out.println("");
	}
	
	public void eliminateKey()
	{
		//eliminateUpdateAvg();
		
		elimateLowerThan(3);
		
		Set<String> keys = keyVisitMap.keySet();
		Set<String> rmKeys = new HashSet<String>();
		for(String aKey : keyPartitions.keySet())
		{
			if(!keys.contains(aKey))
				rmKeys.add(aKey);
		}
		for(String rmKey : rmKeys)
			keyPartitions.remove(rmKey);
	}
	
	private void elimateLowerThan(int bound)
	{
		int keyNumber = keyVisitMap.size();
		if(keyNumber <= bound)
			return;
		
		HashMap<String, Integer> newKVMap = new HashMap<String, Integer>();
		for(String key : keyVisitMap.keySet())
		{
			if(newKVMap.size() < bound)
				newKVMap.put(key, keyVisitMap.get(key));
			else
			{
				String eKey = null;
				int count = Integer.MAX_VALUE;
				for(String newKey : newKVMap.keySet())
				{
					if(newKVMap.get(newKey) < count)
					{
						eKey = newKey;
						count = newKVMap.get(newKey);
					}
				}
				if(count < keyVisitMap.get(key))
				{
					newKVMap.remove(eKey);
					newKVMap.put(key, keyVisitMap.get(key));
				}
			}
		}
		keyVisitMap = newKVMap;
	}
	
	private void eliminateUpdateAvg()
	{
		int totalVisit = 0;
		int keyNumber = keyVisitMap.size();
		
		for(Integer visitCount : keyVisitMap.values())
			totalVisit += visitCount;
		
		if(keyNumber == 0)
			return;
		
		int eliminateTH = totalVisit / keyNumber;
		List<String> eliminateKey = new ArrayList<String>();
		for(String aKey : keyVisitMap.keySet())
		{
			if(keyVisitMap.get(aKey) < eliminateTH)
				eliminateKey.add(aKey);
		}
		
		for(int i = 0; i < eliminateKey.size(); i++)
		{
			keyVisitMap.remove(eliminateKey.get(i));
		}
	}
	
	public String getName()
	{
		return tableName;
	}
	
	public KeyPartition getKeyPartition(String key)
	{
		if(keyVisitMap.get(key) == null)
			keyVisitMap.put(key, 1);
		else
			keyVisitMap.put(key, keyVisitMap.get(key) + 1);
		
		return keyPartitions.get(key);
	}
	
}

class KeyPartition
{
	private String key;
	private List<Predicate> predicates = new ArrayList<Predicate>();
	private Integer min = null;
	private Integer max = null;
	
	public KeyPartition(String key, Integer min, Integer max)
	{
		this.key = key;
		this.max = max;
		this.min = min;
		
		Predicate aPredicate = new Predicate(min, max);
		predicates.add(aPredicate);
	}
	
	public String getName()
	{
		return key;
	}
	
	public Integer getPartitionCount()
	{
		return predicates.size();
	}
	
	public boolean addPredicate(Predicate aPredicate)
	{
		if(aPredicate.getMin() == null)
		{
			int aMax = aPredicate.getMax();
			for(int i = 0; i < predicates.size(); i++)
			{
				if(predicates.get(i).getMax() == aMax)
					return false;
				else if(predicates.get(i).getMax() > aMax)
				{
					Predicate oldPredicate = predicates.get(i);
					
					aPredicate.setMin(oldPredicate.getMin());
					oldPredicate.setMin(aMax);
					predicates.add(i, aPredicate);
					return true;
				}
			}
		}
		else if(aPredicate.getMax() == null)
		{
			int aMin = aPredicate.getMin();
			for(int i = 0; i < predicates.size(); i++)
			{
				if(predicates.get(i).getMin() == aMin)
					return false;
				else if(predicates.get(i).getMin() < aMin)
				{
					Predicate oldPredicate = predicates.get(i);
					
					aPredicate.setMin(oldPredicate.getMax());
					oldPredicate.setMax(aMin);
					predicates.add(i+1, aPredicate);
					return true;
				}
			}
		}
		else
		{
			int aMin = aPredicate.getMin();
			int aMax = aPredicate.getMax();
			
			for (int i = 0; i < predicates.size(); i++) 
			{
				Predicate oldPredicate = predicates.get(i);
				if (aMin + 1 <= oldPredicate.getMax() && 
						aMin - 1 >= oldPredicate.getMin()) 
				{
					if (aMax < oldPredicate.getMax()) 
					{
						Predicate first = new Predicate(aMin, aMax);
						Predicate second = new Predicate(aMax, oldPredicate.getMax());
						oldPredicate.setMax(aMin);
						
						predicates.add(i + 1, second);
						predicates.add(i + 1, first);
						return true;// it is important, avoid counting aMax again
					} 
					else 
					{
						Predicate first = new Predicate(aMin, oldPredicate.getMax());
						oldPredicate.setMax(aMin);
						predicates.add(i + 1, first);
					}
				}
				if(aMax +1 <= oldPredicate.getMax() &&
						aMax - 1 >= oldPredicate.getMin())
				{
					Predicate first = new Predicate(oldPredicate.getMin(), aMax);
					oldPredicate.setMin(aMax);
					predicates.add(i, first);
					return true;
				}
			}
		}
		return false;
	}
	
	public void listPartitions(BufferedWriter out) throws IOException
	{
		for(int i = 0; i < predicates.size(); i++)
		{
			Predicate aPredicate = predicates.get(i);
			out.write(aPredicate.getMin() + ":" + aPredicate.getMax() + "\t");
		}
		out.write("\t");
	}
	
	public List<Predicate> getPredicates()
	{
		return predicates;
	}
}

class MinTerm
{
	List<Predicate> terms = null;
	int node = 0;
	int pos = 0;
	HashMap<Integer, Integer> edgeCount = new HashMap<Integer, Integer>();
	
	public MinTerm(MinTerm oldMinTerm, Predicate newPredicate)
	{
		terms = new ArrayList<Predicate>();
		
		List<Predicate> oldTerms = oldMinTerm.getTerms();
		for(int i = 0; i < oldTerms.size(); i++)
			terms.add(new Predicate(oldTerms.get(i)));
		
		terms.add(new Predicate(newPredicate));
	}
	
	public int getEstimatedSize()
	{
		int size = 0;
		for(int i = 0; i < terms.size(); i++)
		{
			int gap = terms.get(i).getMax() - terms.get(i).getMin();
			if(gap > Integer.MAX_VALUE / 5 || gap < 0)
				gap = 1;
			size += gap;
		}
		if(size < 0)
			System.err.println("Err");
		return size;
	}
	
	public void setNode(int node)
	{
		this.node = node;
	}
	public int getNode()
	{
		return node;
	}
	
	public HashMap<Integer, Integer> getNeighbour()
	{
		return edgeCount;
	}
	
	public int renewPos(HashMap<Integer, Integer> posMap)
	{
		int edgeNum = 0;
		HashMap<Integer, Integer> newEdgeCount = new HashMap<Integer, Integer>();
		for(Integer edge : edgeCount.keySet())
		{
			if(posMap.get(edge) == null)
				System.err.println("Error: no map for: " + edge);
			else
				newEdgeCount.put(posMap.get(edge), edgeCount.get(edge));
			
			edgeNum++;
		}
		edgeCount = newEdgeCount;
		
		return edgeNum;
	}
	public boolean combinePre(MinTerm toCombine)
	{
		List<Predicate> toComList = toCombine.getTerms();
		
		if(toCombine.getTerms().size() != terms.size())
			return false;
		
		for(int i = terms.size() - 1; i >= 0; i--)
		{
			Predicate toComP = toComList.get(i);
			Predicate thisP = terms.get(i);
			if(toComP.getMax().equals(thisP.getMin()))
			{
				thisP.setMin(toComP.getMin());
				return true;
			}
			else if(toComP.getMax().equals(thisP.getMax()) && toComP.getMin().equals(thisP.getMin()))
				continue;
			else
				return false;
		}
		return false;
	}
	
	public boolean combineNext(MinTerm toCombine)
	{
		List<Predicate> toComList = toCombine.getTerms();
		
		if(toCombine.getTerms().size() != terms.size())
			return false;
		
		for(int i = terms.size() - 1; i >= 0; i--)
		{
			Predicate toComP = toComList.get(i);
			Predicate thisP = terms.get(i);
			if(toComP.getMin().equals(thisP.getMax()))
			{
				thisP.setMax(toComP.getMax());
				return true;
			}
			else if(toComP.getMax().equals(thisP.getMax()) && toComP.getMin().equals(thisP.getMin()))
				continue;
			else
				return false;
		}
		return false;
	}
	
	public int getNeibourSize()
	{
		return edgeCount.size();
	}
	public void setPos(int pos)
	{
		this.pos = pos;
	}
	public int getPos()
	{
		return pos;
	}
	
	public boolean match(List<Predicate> searchMT)
	{
		boolean match = true;
		
		for(int i = 0; i < terms.size(); i++)
			if( !terms.get(i).match(searchMT.get(i)) )
				return false;
		
		return match;
	}
	public void addConnect(int connectTo)
	{
		if(edgeCount.get(connectTo) == null)
			edgeCount.put(connectTo, 1);
		else
			edgeCount.put(connectTo, edgeCount.get(connectTo) + 1);
	}
	
	
	public MinTerm(Predicate aPredicate)
	{
		terms = new ArrayList<Predicate>();
		terms.add(aPredicate);
	}
	
	public List<Predicate> getTerms()
	{
		return terms;
	}
	
	public void listMinTerm()
	{
		for(int i = 0; i < terms.size(); i++)
			System.out.print(terms.get(i).toString() + "\t");
		
		for(Integer neighbour : edgeCount.keySet())
		{
			System.out.print(neighbour + "->" + edgeCount.get(neighbour) + "\t");
		}
		System.out.println("");
	}
	
	public int hashCode()
	{
		int hashCode = 0;
		for(int i = 0; i < terms.size(); i++)
			hashCode = hashCode * terms.get(i).hashCode();
		
		return Math.abs(hashCode);
	}
	
	public String toString()
	{
		String out = "";
		for(int i = 0; i < terms.size(); i++)
			out += terms.get(i).toString() + "\t";
		
		return out;
	}
}

class Predicate
{
	private Integer min = null;
	private Integer max = null;
	
	public Predicate()
	{}
	
	public Predicate(int min, int max)
	{
		this.min = min;
		this.max = max;
	}
	
	public Predicate(Predicate copyP)
	{
		min = copyP.min;
		max = copyP.max;
	}
	
	
	
	public Integer getMin()
	{
		return min;
	}
	public Integer getMax()
	{
		return max;
	}
	
	public void setMin(int min)
	{
		this.min = min;
	}
	public void setMax(int max)
	{
		this.max = max;
	}
	
	public boolean match(Predicate searchP)
	{
		int sMin = searchP.getMin();
		int sMax = searchP.getMax();
		
		//Note that, we add this condition manually
		if(sMin >= sMax)
			return true;
		
		if(sMin >= min && sMax <= max)
			return true;
		else
			return false;
	}
	
	public int hashCode()
	{
		return (min + max) / 2;
	}
	
	public String toString()
	{
		return min +":"+ max;
	}
}
