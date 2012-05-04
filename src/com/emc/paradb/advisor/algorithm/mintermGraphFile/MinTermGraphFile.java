package com.emc.paradb.advisor.algorithm.mintermGraphFile;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
public class MinTermGraphFile implements PlugInterface
{
	Connection conn = null;
	Workload<Transaction<Object>> workload = null;
	DBData dbData = null;
	int nodes = 0;
	int partitionCount = 0;
	
	HashMap<String, TablePartition> tablePartitions = null;
	
	List<String> tableList = null;
	HashMap<String, List<String>> tableKeyMap = null;
	
	Graph minTermGraph = null;
	
	
	private static boolean partitioned = false;
	
	@Override
	public boolean accept(Connection conn, Workload<Transaction<Object>> workload,
			DBData dbData, int nodes) 
	{
		//if already partitioned, start the refine process.
		if(partitioned && nodes == this.nodes)
		{
			try 
			{
				refine();
			} 
			catch (Exception e) 
			{
				e.printStackTrace();
			}
			return true;
		}
		
		//partittion the graph for the first time
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
			partitioned = true;
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		return true;
	}
	
	
	private void refine() throws Exception
	{
		//refine partition
		//minTermGraph.refinePartition();

		tableKeyMap.clear();
		for (int i = 0; i < tableList.size(); i++) {
			List<String> keyList = new ArrayList<String>();
			keyList.add("undefined");
			tableKeyMap.put(tableList.get(i), keyList);
		}

		List<String> keyList = new ArrayList<String>();
		keyList.add("replicate");
		tableKeyMap.put("item", keyList);

		//minTermGraph.display();
	}
	
	private void workload2Graph() throws Exception
	{
		
		//construct minterm list
		HashMap<String, Integer> tableStartPos = new HashMap<String, Integer>();
		HashMap<String, Integer> tableEndPos = new HashMap<String, Integer>();
		List<MinTerm> minTermList = new ArrayList<MinTerm>();
		
		serialize(tableStartPos, tableEndPos, minTermList);
		
		//construct the graph with the minterm node list
		minTermGraph = new Graph(tableStartPos, tableEndPos, minTermList);	
		
		
		//add edges among minterms
		for(Transaction<Object> aTran : workload)
			explainTran(aTran);	
		
		minTermGraph.initGraphFile();
		
		//combine unvisited minterms
		combine();

/*		
		for(int i = 0; i < tableList.size(); i++)
		{
			String tableName = tableList.get(i);
			TablePartition aTablePartition = tablePartitions.get(tableName);
			System.out.print(aTablePartition.getName() + ": ");
			List<String> keyList = aTablePartition.getKeyList();
			for(int j = 0; j < keyList.size(); j++)
				System.out.print(aTablePartition.getKeyPartition(keyList.get(j)).getName() + "\t");
		}
*/	
		//partition the minterm graph
		minTermGraph.partitionGraph(nodes);
		
		//refine partition
		//minTermGraph.refinePartition();
		
		for(int i = 0; i < tableList.size(); i++)
		{
			List<String> keyList = new ArrayList<String>();
			keyList.add("undefined");
			tableKeyMap.put(tableList.get(i), keyList);
		}
		
		List<String> keyList = new ArrayList<String>();
		keyList.add("replicate");
		tableKeyMap.put("item", keyList);
		
		//minTermGraph.display();
	}
	
	private void combine() throws IOException
	{
		List<MinTerm> uncombined = new ArrayList<MinTerm>();
		for(int i = 0; i < tableList.size(); i++)
		{
			String tableName = tableList.get(i);
			uncombined.addAll(minTermGraph.combine(tableName));
		}
	/*
		if(uncombined.size() == 1)
			System.err.println("not combined!!!!!!!!!!");
		
		RandomAccessFile inOut = new RandomAccessFile(GraphFile.getGraphFile(), "rw");
		inOut.seek(inOut.length());
		
		for(int i = 0; i < uncombined.size(); i++)
		{
			MinTerm aMinTerm = uncombined.get(i);
			aMinTerm.setOffset(inOut.getFilePointer());
			aMinTerm.setEdgeCnt(uncombined.size()-1);
			
			inOut.write(String.valueOf(aMinTerm.getPos()-1).getBytes());
			for(int j = 0; j < uncombined.size(); j++)
				if(i != j)
					inOut.write(("\t" + (uncombined.get(j).getPos()-1) + "\t1").getBytes());
			inOut.write("\n".getBytes());
		}
		inOut.close();*/
	}
	
	/*
	 * translate predicates into minTerms lists.
	 * Each table has its start position and end position in the list.
	 */
	private void serialize(HashMap<String, Integer> tableStartPos,
							HashMap<String, Integer> tableEndPos,
							List<MinTerm> minTermList)
	{
		tableList = new ArrayList<String>();
		
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
	
		setPartitionSize();
	}
	
	private void setPartitionSize()
	{
		for(TablePartition aTablePartition : tablePartitions.values())
			aTablePartition.setKeyPartitionSize();
	}
	private void eliminateKey()
	{
		for(TablePartition aTablePartition : tablePartitions.values())
		{
			aTablePartition.eliminateKey();
			aTablePartition.updateKeyBound();
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
	
	/**
	 * explain which minterms are accessed together. Add edges among them
	 * @param tableKeyValueMap
	 */
	private void explainTableKeyValueMap(HashMap<String, List<KeyValuePair>> tableKeyValueMap)
	{
		Set<Integer> visitNodes = new HashSet<Integer>();
		
		for(String tableName : tableKeyValueMap.keySet())
		{
			List<KeyValuePair> kvList = tableKeyValueMap.get(tableName);
			
			List<Predicate> searchMT = initSearchMinTerm(tableName, kvList);
			if(searchMT == null)
				continue;
			
			Set<Integer> matchNodes = minTermGraph.match(searchMT, tableName);
			visitNodes.addAll(matchNodes);
		}
		try 
		{
			minTermGraph.addConnect(visitNodes);
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
	
		
		List<Integer> placement = minTermGraph.searchPlacement(searchMT, tableName);
		nodeList.addAll(placement);
		
	
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
	
	private final int retainTopK = 3;
	
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
				int min = minResult.getInt(1);
				
				ResultSet maxResult = stmt.executeQuery("select max(" + aKey +") " +
														"from " + QueryPrepare.prepare(tableName) + ";");
				maxResult.next();
				int max = maxResult.getInt(1); 
				
				ResultSet cardResult = stmt.executeQuery("select count(*) " + 
												   "from " + QueryPrepare.prepare(tableName) + ";");
				cardResult.next();
				int card = cardResult.getInt(1);
				
				KeyPartition aKeyPartition = new KeyPartition(aKey, min, max + 1, card);
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
	
	public void setKeyPartitionSize()
	{
		for(KeyPartition aKey : keyPartitions.values())
			aKey.setPartitionSize();
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
	
	public void updateKeyBound()
	{
		for(KeyPartition aKey : keyPartitions.values())
			aKey.setBound();
	}
	
	public void eliminateKey()
	{
		//eliminateUpdateAvg();
		
		elimateLowerThan(retainTopK);
		
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








/**
 * 
 * @author Xin Pan
 *maintain partition result of a key
 *
 *
 */
class KeyPartition
{
	private String key;
	private List<Predicate> predicates = new ArrayList<Predicate>();
	
	private int min;
	private int max;
	private int card;
	
	private int minVisit = Integer.MAX_VALUE;
	private int maxVisit = Integer.MIN_VALUE;
	
	public KeyPartition(String key, int min, int max, int card)
	{
		this.key = key;
		this.min = min;
		this.max = max;
		this.card = card;
		
		Predicate aPredicate = new Predicate(Integer.MIN_VALUE/2, Integer.MAX_VALUE/2);
		predicates.add(aPredicate);
	}
	
	public void setPartitionSize()
	{
		for(int i = 0; i < predicates.size(); i++)
		{
			int pMin = predicates.get(i).getMin();
			int pMax = predicates.get(i).getMax();
			int estimateSize = (int)(((double)(pMax - pMin))/(max - min) * card);
			estimateSize = estimateSize == 0 ? 1 : estimateSize;
			predicates.get(i).setSize(estimateSize);
		}
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
		updateBound(aPredicate);
		
		if(aPredicate.getMin() == null)
		{
			final int aMax = aPredicate.getMax();
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
			final int aMin = aPredicate.getMin();
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
			final int aMin = aPredicate.getMin();
			final int aMax = aPredicate.getMax();
			
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
	
	private void updateBound(Predicate aPredicate)
	{
		if(aPredicate.getMax() != null && aPredicate.getMax() > maxVisit)
			maxVisit = aPredicate.getMax();
		if(aPredicate.getMin() != null && aPredicate.getMin() < minVisit)
			minVisit = aPredicate.getMin();
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
	
	public void setBound()
	{
		if(min > minVisit)
			min = minVisit;
		if(max < maxVisit)
			max = maxVisit;
		
		predicates.get(0).setMin(min);
		predicates.get(predicates.size() - 1).setMax(max);
	}
}