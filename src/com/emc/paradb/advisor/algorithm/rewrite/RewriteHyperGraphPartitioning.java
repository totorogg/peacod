package com.emc.paradb.advisor.algorithm.rewrite;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
 * This algorithm extract all predicates from workload and construct minTerms.
 * Each minTerm is regarded as a graph node. if two nodes are visited in a
 * transaction, an edge is added between two nodes
 * 
 * after graph construction, we partition it with METIS
 * 
 * 
 * @author Xin Pan
 * 
 */
public class RewriteHyperGraphPartitioning implements PlugInterface {
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
	//[tag xiaoyan] refine count
	private int refineCount = 10;

	@Override
	//[tag xiaoyan: the interface of peacod for a partitioning scheme]
	public boolean accept(Connection conn,
			Workload<Transaction<Object>> workload, DBData dbData, int nodes) {

		// partition the graph for the first time
		this.conn = conn;
		this.workload = workload;
		this.dbData = dbData;
		this.nodes = nodes;
		partitionCount = 0;

		tableKeyMap = new HashMap<String, List<String>>();
		tablePartitions = new HashMap<String, TablePartition>();

		try {
			//[tag xiaoyan] clear test/*
			//Process p = Runtime.getRuntime().exec("rm test/* ");
			//p.waitFor();
			setPartition();
			workload2Graph();
			partitioned = true;
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		return true;
	}

	private void refine() throws Exception {
		// refine partition
		// minTermGraph.refinePartition();

		tableKeyMap.clear();
		for (int i = 0; i < tableList.size(); i++) {
			List<String> keyList = new ArrayList<String>();
			keyList.add("undefined");
			tableKeyMap.put(tableList.get(i), keyList);
		}

		List<String> keyList = new ArrayList<String>();
		keyList.add("replicate");
		tableKeyMap.put("item", keyList);

		// minTermGraph.display();
	}
	
	//[tag xiaoyan] hyper-graph refinement
	/*
	 * 1. find the top-k or top-k% vertex that has margest node weight
	 * 2. add each vertex to the end of adjacent list
	 * 3. split each vertex to be 2 in each edge
	 * 4. modify the node weight of the split nodes
	 */
	private void hyperGraphRefinement(Graph g) {
		class Mnode {
			int weight;
			int index;
			public Mnode(int weight, int index) {
				this.weight = weight;
				this.index = index;
			}
		}
		//sort the adjacent list;
		int index = 0;
		Comparator comp = new Comparator() {
			public int compare(Object o1, Object o2) {
				Mnode n1 = (Mnode) o1;
				Mnode n2 = (Mnode) o2;
				if (n1.weight < n2.weight)
					return 1;
				else 
					return 0;
			}
		};
		Vector<Mnode> v = new Vector();
		for (MinTerm m: g.adjacencyList) {
			int weight = g.getMintermNodeWeight(m);
			Mnode n = new Mnode(weight, index);
			v.addElement(n);
			index++;
		}
		Collections.sort(v, comp);
		// find the top k elements
		int k = 10;
		for (int i = 0; i< k; i++) {
			index = v.elementAt(i).index;
			// add this vertex to the end of the adjacent list
			MinTerm m = g.adjacencyList.get(index);
			MinTerm newm = new MinTerm(m);
			newm.prop /= 2.0;
			m.prop /= 2.0;
			g.adjacencyList.add(newm);
			// travaerse each hyper edge
			Set<Set<Integer>> keyset = new HashSet();
			for (Set<Integer> ge : g.hyperEdge.keySet()) {
				keyset.add(ge);
			}
			for (Set<Integer> ge : keyset){
				if (ge.contains(index)) {
					int cnt = g.hyperEdge.get(ge);
				
					g.hyperEdge.remove(ge);
					//g.hyperEdge.put(ge, 0);
					ge.add(g.adjacencyList.size() - 1);
					g.hyperEdge.put(ge, cnt);
				}
			}
		}
	}

	private void workload2Graph() throws Exception {

		// construct minterm list
		HashMap<String, Integer> tableStartPos = new HashMap<String, Integer>();
		HashMap<String, Integer> tableEndPos = new HashMap<String, Integer>();
		List<MinTerm> minTermList = new ArrayList<MinTerm>();
		//[tag xiaoyan] the hyper edge
		HashMap<Set<Integer>, Integer> hyperEdge = new HashMap<Set<Integer>, Integer>();

		//[tag xiaoyan] cross the minterm item in a table to be the minterm
		serialize(tableStartPos, tableEndPos, minTermList);

		// construct the graph with the minterm node list
		minTermGraph = new Graph(tableStartPos, tableEndPos, minTermList, hyperEdge);

		// add edges among minterms
		for (Transaction<Object> aTran : workload)
			explainTran(aTran);

		//[tag xiaoyan] not need neither ?
		//minTermGraph.initGraphFile();

		// combine unvisited minterms
		// combine();

		/*
		 * for(int i = 0; i < tableList.size(); i++) { String tableName =
		 * tableList.get(i); TablePartition aTablePartition =
		 * tablePartitions.get(tableName);
		 * System.out.print(aTablePartition.getName() + ": "); List<String>
		 * keyList = aTablePartition.getKeyList(); for(int j = 0; j <
		 * keyList.size(); j++)
		 * System.out.print(aTablePartition.getKeyPartition(
		 * keyList.get(j)).getName() + "\t"); }
		 */
		// partition the minterm graph
		//[tag xiaoyan] add graph refinement function
		minTermGraph.partitionGraph(nodes);
		while (this.refineCount >= 0) {
			this.refineCount--;
			this.hyperGraphRefinement(minTermGraph);
			minTermGraph.partitionGraph(nodes);
			
			System.out.println("refine count = " + this.refineCount);
		};

		// refine partition
		// minTermGraph.refinePartition();

		for (int i = 0; i < tableList.size(); i++) {
			List<String> keyList = new ArrayList<String>();
			keyList.add("undefined");
			tableKeyMap.put(tableList.get(i), keyList);
		}

		List<String> keyList = new ArrayList<String>();
		keyList.add("replicate");
		tableKeyMap.put("item", keyList);

		// minTermGraph.display();
	}

	private void combine() throws IOException {
		List<MinTerm> uncombined = new ArrayList<MinTerm>();
		for (int i = 0; i < tableList.size(); i++) {
			String tableName = tableList.get(i);
			uncombined.addAll(minTermGraph.combine(tableName));
		}
		/*
		 * if(uncombined.size() == 1)
		 * System.err.println("not combined!!!!!!!!!!");
		 * 
		 * RandomAccessFile inOut = new
		 * RandomAccessFile(GraphFile.getGraphFile(), "rw");
		 * inOut.seek(inOut.length());
		 * 
		 * for(int i = 0; i < uncombined.size(); i++) { MinTerm aMinTerm =
		 * uncombined.get(i); aMinTerm.setOffset(inOut.getFilePointer());
		 * aMinTerm.setEdgeCnt(uncombined.size()-1);
		 * 
		 * inOut.write(String.valueOf(aMinTerm.getPos()-1).getBytes()); for(int
		 * j = 0; j < uncombined.size(); j++) if(i != j) inOut.write(("\t" +
		 * (uncombined.get(j).getPos()-1) + "\t1").getBytes());
		 * inOut.write("\n".getBytes()); } inOut.close();
		 */
	}

	/*
	 * translate predicates into minTerms lists. Each table has its start
	 * position and end position in the list.
	 */
	private void serialize(HashMap<String, Integer> tableStartPos,
			HashMap<String, Integer> tableEndPos, List<MinTerm> minTermList) {
		tableList = new ArrayList<String>();

		for (TablePartition aTablePartition : tablePartitions.values()) {
			tableList.add(aTablePartition.getName());

			tableStartPos.put(aTablePartition.getName(), partitionCount);
			partitionCount += aTablePartition.serialize(minTermList);
			tableEndPos.put(aTablePartition.getName(), partitionCount);
		}
	}

	//[tag xiaoyan]: extract where clauses from sqls
	private void extractTran(Transaction<Object> aTran) {
		for (Object statement : aTran) {

			if (statement instanceof SelectAnalysisInfo) {
				SelectAnalysisInfo select = (SelectAnalysisInfo) statement;

				for (WhereKey key : select.getWhereKeys()) {
					String tableName = key.getTableName();
					TablePartition aTablePartition = tablePartitions
							.get(tableName);
					if (aTablePartition == null)
						continue;

					String aKey = key.getKeyName();
					KeyPartition aKeyPartition = aTablePartition
							.getKeyPartition(aKey);
					if (aKeyPartition == null)
						continue;

					Predicate aPredicate = extractPredicate(aKey,
							key.getKeyValue(), key.getRange());
					if (aPredicate != null)
						aKeyPartition.addPredicate(aPredicate);
				}
			} else if (statement instanceof UpdateAnalysisInfo) {
				UpdateAnalysisInfo update = (UpdateAnalysisInfo) statement;
				for (WhereKey key : update.getWhereKeys()) {
					String tableName = key.getTableName();
					TablePartition aTablePartition = tablePartitions
							.get(tableName);
					if (aTablePartition == null)
						continue;

					String aKey = key.getKeyName();
					KeyPartition aKeyPartition = aTablePartition
							.getKeyPartition(aKey);
					if (aKeyPartition == null)
						continue;

					Predicate aPredicate = extractPredicate(aKey,
							key.getKeyValue(), key.getRange());
					if (aPredicate != null)
						aKeyPartition.addPredicate(aPredicate);
				}
			} else if (statement instanceof InsertAnalysisInfo) {
				InsertAnalysisInfo insert = (InsertAnalysisInfo) statement;

				for (String key : insert.getKeyValueMap().keySet()) {
					String tableName = insert.getTable();
					TablePartition aTablePartition = tablePartitions
							.get(tableName);
					if (aTablePartition == null)
						continue;

					KeyPartition aKeyPartition = aTablePartition
							.getKeyPartition(key);
					if (aKeyPartition == null)
						continue;

					Predicate aPredicate = extractPredicate(key, insert
							.getKeyValueMap().get(key), Range.EQUAL);
					if (aPredicate != null)
						aKeyPartition.addPredicate(aPredicate);
				}
			} else if (statement instanceof DeleteAnalysisInfo) {
				DeleteAnalysisInfo delete = (DeleteAnalysisInfo) statement;
				for (WhereKey key : delete.getWhereKeys()) {
					String tableName = key.getTableName();
					TablePartition aTablePartition = tablePartitions
							.get(tableName);
					if (aTablePartition == null)
						continue;

					String aKey = key.getKeyName();
					KeyPartition aKeyPartition = aTablePartition
							.getKeyPartition(aKey);
					if (aKeyPartition == null)
						continue;

					Predicate aPredicate = extractPredicate(aKey,
							key.getKeyValue(), key.getRange());
					if (aPredicate != null)
						aKeyPartition.addPredicate(aPredicate);
				}
			}
		}
	}

	private void setPartition() {
		//[tag xiaoyan]: extract the where clauses form sqls
		preparePredicates();

		for (Transaction<Object> aTran : workload)
			extractTran(aTran);

		//[tag xiaoyan] only use the first topk = 3 attributes for a given table
		eliminateKey();

		setPartitionSize();
	}

	private void setPartitionSize() {
		for (TablePartition aTablePartition : tablePartitions.values())
			aTablePartition.setKeyPartitionSize();
	}

	private void eliminateKey() {
		for (TablePartition aTablePartition : tablePartitions.values()) {
			aTablePartition.eliminateKey();
			aTablePartition.updateKeyBound();
		}
	}

	private void listPartitions(BufferedWriter out) throws IOException {
		for (TablePartition aTablePartition : tablePartitions.values()) {
			out.write(aTablePartition.getName() + "\t");
			aTablePartition.listPartitions(out);
		}
	}

	private void explainTran(Transaction<Object> aTran) {

		//HashMap<String, List<KeyValuePair>> tableKeyValueMap = new HashMap<String, List<KeyValuePair>>();
		Set<Integer> visitNodes = new HashSet<Integer>();

		for (Object statement : aTran) {
			HashMap<String, List<KeyValuePair>> tableKeyValueMap = new HashMap<String, List<KeyValuePair>>();
			
			if (statement instanceof SelectAnalysisInfo) {
				SelectAnalysisInfo select = (SelectAnalysisInfo) statement;

				for (WhereKey key : select.getWhereKeys()) {
					String tableName = key.getTableName();
					if (tablePartitions.get(tableName) == null
							|| tablePartitions.get(tableName).getKeyPartition(
									key.getKeyName()) == null)
						continue;

					if (tableKeyValueMap.get(tableName) == null) {
						List<KeyValuePair> aKeyValueList = new ArrayList<KeyValuePair>();
						KeyValuePair aKVPair = new KeyValuePair(
								key.getKeyName(), key.getKeyValue());
						aKeyValueList.add(aKVPair);
						tableKeyValueMap.put(tableName, aKeyValueList);
					} else {
						List<KeyValuePair> aKeyValueList = tableKeyValueMap
								.get(tableName);
						KeyValuePair aKVPair = new KeyValuePair(
								key.getKeyName(), key.getKeyValue());
						aKeyValueList.add(aKVPair);
						tableKeyValueMap.put(tableName, aKeyValueList);
					}

				}
			} else if (statement instanceof UpdateAnalysisInfo) {
				UpdateAnalysisInfo update = (UpdateAnalysisInfo) statement;
				for (WhereKey key : update.getWhereKeys()) {
					String tableName = key.getTableName();
					if (tablePartitions.get(tableName) == null
							|| tablePartitions.get(tableName).getKeyPartition(
									key.getKeyName()) == null)
						continue;

					if (tableKeyValueMap.get(tableName) == null) {
						List<KeyValuePair> aKeyValueList = new ArrayList<KeyValuePair>();
						KeyValuePair aKVPair = new KeyValuePair(
								key.getKeyName(), key.getKeyValue());
						aKeyValueList.add(aKVPair);
						tableKeyValueMap.put(tableName, aKeyValueList);
					} else {
						List<KeyValuePair> aKeyValueList = tableKeyValueMap
								.get(tableName);
						KeyValuePair aKVPair = new KeyValuePair(
								key.getKeyName(), key.getKeyValue());
						aKeyValueList.add(aKVPair);
						tableKeyValueMap.put(tableName, aKeyValueList);
					}

				}
			} else if (statement instanceof InsertAnalysisInfo) {
				InsertAnalysisInfo insert = (InsertAnalysisInfo) statement;

				for (String key : insert.getKeyValueMap().keySet()) {
					String tableName = insert.getTable();
					if (tablePartitions.get(tableName) == null
							|| tablePartitions.get(tableName).getKeyPartition(
									key) == null)
						continue;

					if (tableKeyValueMap.get(tableName) == null) {
						List<KeyValuePair> aKeyValueList = new ArrayList<KeyValuePair>();
						KeyValuePair aKVPair = new KeyValuePair(key, insert
								.getKeyValueMap().get(key));
						aKeyValueList.add(aKVPair);
						tableKeyValueMap.put(tableName, aKeyValueList);
					} else {
						List<KeyValuePair> aKeyValueList = tableKeyValueMap
								.get(tableName);
						KeyValuePair aKVPair = new KeyValuePair(key, insert
								.getKeyValueMap().get(key));
						aKeyValueList.add(aKVPair);
						tableKeyValueMap.put(tableName, aKeyValueList);
					}
				}
			} else if (statement instanceof DeleteAnalysisInfo) {
				DeleteAnalysisInfo delete = (DeleteAnalysisInfo) statement;
				for (WhereKey key : delete.getWhereKeys()) {
					String tableName = key.getTableName();
					if (tablePartitions.get(tableName) == null
							|| tablePartitions.get(tableName).getKeyPartition(
									key.getKeyName()) == null)
						continue;

					if (tableKeyValueMap.get(tableName) == null) {
						List<KeyValuePair> aKeyValueList = new ArrayList<KeyValuePair>();
						KeyValuePair aKVPair = new KeyValuePair(
								key.getKeyName(), key.getKeyValue());
						aKeyValueList.add(aKVPair);
						tableKeyValueMap.put(tableName, aKeyValueList);
					} else {
						List<KeyValuePair> aKeyValueList = tableKeyValueMap
								.get(tableName);
						KeyValuePair aKVPair = new KeyValuePair(
								key.getKeyName(), key.getKeyValue());
						aKeyValueList.add(aKVPair);
						tableKeyValueMap.put(tableName, aKeyValueList);
					}

				}
			}
			visitNodes.addAll(explainTableKeyValueMap(tableKeyValueMap));
		}
		//explainTableKeyValueMap(tableKeyValueMap);
		try {
			minTermGraph.addConnect(visitNodes);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * explain which minterms are accessed together. Add edges among them
	 * 
	 * @param tableKeyValueMap
	 */
	private Set<Integer> explainTableKeyValueMap(
			HashMap<String, List<KeyValuePair>> tableKeyValueMap) {
		Set<Integer> visitNodes = new HashSet<Integer>();

		for (String tableName : tableKeyValueMap.keySet()) {
			List<KeyValuePair> kvList = tableKeyValueMap.get(tableName);

			List<Predicate> searchMT = initSearchMinTerm(tableName, kvList);
			if (searchMT == null)
				continue;

			Set<Integer> matchNodes = minTermGraph.match(searchMT, tableName);
			visitNodes.addAll(matchNodes);
		}
		return visitNodes;
	}

	private List<Predicate> initSearchMinTerm(String tableName,
			List<KeyValuePair> kvList) {
		List<String> keyList = tablePartitions.get(tableName).getKeyList();
		List<Predicate> predicates = new ArrayList<Predicate>();
		int count = 0;

		for (int i = 0; i < keyList.size(); i++) {
			boolean found = false;
			for (int j = 0; j < kvList.size(); j++) {
				if (keyList.get(i).equals(kvList.get(j).getKey())) {
					KeyValuePair kvPair = kvList.get(j);
					if (kvPair.getValue() == null)
						return null;
					//[tag xiaoyan] support string value
					int min = 0;
					try {
						min = Integer.valueOf(kvPair.getValue());
					} catch (Exception e) {
						predicates.add(new Predicate(kvPair.getValue()));
						found = true;
						break;
					}
					//[tag xiaoyan] integer value
					int max = min + 1;

					predicates.add(new Predicate(min, max));// note that we
															// consider equal
															// only
					found = true;
					break;
				}
			}
			if (!found) {
				predicates.add(new Predicate(Integer.MAX_VALUE,
						Integer.MIN_VALUE));
				count++;
			}
		}
		if (count == keyList.size())
			return null;
		else
			return predicates;
	}

	public Predicate extractPredicate(String keyName, String keyValue,
			Range range) {
		if (keyValue == null) {
			return null;
		}
		Predicate newPredicate = new Predicate();

		int value = 0;
		int type = 0;
		//[tag xiaoyan] currently only support integer?
		try {
			value = Integer.valueOf(keyValue);
		} catch (NumberFormatException e) {
			//[tag xiaoyan] string value
			//newPredicate.setStrVal(keyValue);
			//return newPredicate;
			//value = keyValue.hashCode() % (Integer.MAX_VALUE / 2);
			//return null;
			type = 1;
		}

		newPredicate.setType(type);
		if (type == 1) {
			newPredicate.setStrVal(keyValue);
			return newPredicate;
		}
		
		// for integer value
		if (range == Range.EQUAL) {
			newPredicate.setMax(value + 1);
			newPredicate.setMin(value);
		} else if (range == Range.LARGEEQL) {
			newPredicate.setMin(value);
		} else if (range == Range.LARGER) {
			newPredicate.setMin(value + 1);
		} else if (range == Range.SMALLEQL) {
			newPredicate.setMax(value + 1);
		} else if (range == Range.SMALLER) {
			newPredicate.setMax(value);
		} else {
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

	public void preparePredicates() {
		HashMap<String, TableNode> tables = dbData.getMetaData();
		for (TableNode aTableNode : tables.values()) {
			if (aTableNode.getName().equals("item"))
				continue;

			TablePartition aTablePartition = new TablePartition(aTableNode,
					conn);
			//[tag xiaoyan] for each table, we have a table partition
			tablePartitions.put(aTableNode.getName(), aTablePartition);
		}
	}

	@Override
	public HashMap<String, List<String>> getPartitionKey() {
		// TODO Auto-generated method stub
		return tableKeyMap;
	}

	@Override
	public List<Integer> getNode(List<KeyValuePair> kvPairs) {
		// TODO Auto-generated method stub

		List<Integer> nodeList = new ArrayList<Integer>();

		if (kvPairs.get(0).getValue().equalsIgnoreCase("replicate")) {
			nodeList.add(-2);
			return nodeList;
		}

		else if (kvPairs.get(0).getKey().equals("undefined")) {
			for (int i = 0; i < nodes; i++)
				nodeList.add(0);
			return nodeList;
		}

		String tableName = kvPairs.get(0).getTable();

		List<Predicate> searchMT = initSearchMinTerm(tableName, kvPairs);
		if (searchMT == null) {
			for (int i = 0; i < nodes; i++)
				nodeList.add(-1);
			return nodeList;
		}

		List<Integer> placement = minTermGraph.searchPlacement(searchMT,
				tableName);
		nodeList.addAll(placement);

		if (nodeList.size() == 0)
			nodeList.add(-1);

		return nodeList;
	}

	@Override
	public List<String[]> getSetting() {
		// TODO Auto-generated method stub
		return null;
	}
}

class TablePartition {
	private String tableName;
	//[tag xiaoyan] store each key of a table's partitions
	private HashMap<String, KeyPartition> keyPartitions = new HashMap<String, KeyPartition>();
	HashMap<String, Integer> keyVisitMap = new HashMap<String, Integer>();
	HashMap<String, Integer> keyPartitionCount = new HashMap<String, Integer>();

	int partitionCount = 0;
	List<String> keyList = null;

	private final int retainTopK = 2;

	public TablePartition(String tableName) {
		this.tableName = tableName;
	}

	//[tag xiaoyan] get the partitions for each attributes
	public TablePartition(TableNode aTableNode, Connection conn) {
		tableName = aTableNode.getName();

		Vector<TableAttributes> attrs = aTableNode.getAttrVector();

		for (TableAttributes aAttr : attrs) {
			String aKey = aAttr.getName();
			try {
				Statement stmt = conn.createStatement();
				ResultSet cardResult = stmt.executeQuery("select count(distinct " + aKey + ") "
						+ "from " + QueryPrepare.prepare(tableName) + ";");
				cardResult.next();
				int card = cardResult.getInt(1);
				
				ResultSet countResult = stmt.executeQuery("select count(*) "
						+ "from " + QueryPrepare.prepare(tableName) + ";");
				countResult.next();
				int cnt = countResult.getInt(1);
				
				ResultSet minResult = stmt.executeQuery("select min(" + aKey
						+ ") " + "from " + QueryPrepare.prepare(tableName)
						+ ";");

				int type = minResult.getMetaData().getColumnType(1);
				//[tag xiaoyan] supporting string value
				if (type != Types.INTEGER) {
					KeyPartition aKeyPartition = new KeyPartition(aKey, 0,
							Integer.MAX_VALUE / 2, card, cnt, 1);
					keyPartitions.put(aKey, aKeyPartition);
					continue;
				}

				minResult.next();
				int min = minResult.getInt(1);

				ResultSet maxResult = stmt.executeQuery("select max(" + aKey
						+ ") " + "from " + QueryPrepare.prepare(tableName)
						+ ";");
				maxResult.next();
				int max = maxResult.getInt(1);

				KeyPartition aKeyPartition = new KeyPartition(aKey, min,
						max + 1, card, cnt, 0);
				keyPartitions.put(aKey, aKeyPartition);
			} catch (SQLException e) {
				System.err.println(e.getMessage());
				e.printStackTrace();
			}
		}

	}

	public List<String> getKeyList() {
		return keyList;
	}

	public int serialize(List<MinTerm> minTermList) {
		final int startIndex = minTermList.size();
		keyList = new ArrayList<String>();

		for (KeyPartition aKeyPartition : keyPartitions.values()) {
			keyList.add(aKeyPartition.getName());
			keyPartitionCount.put(aKeyPartition.getName(),
					aKeyPartition.getPartitionCount());

			if (minTermList.size() == startIndex) {
				List<Predicate> predicates = aKeyPartition.getPredicates();

				for (int i = 0; i < predicates.size(); i++) {
					MinTerm aMinTerm = new MinTerm(predicates.get(i));
					aMinTerm.setPos(++partitionCount + startIndex);
					minTermList.add(aMinTerm);
				}
			} else {
				partitionCount = 0;
				List<Predicate> predicates = aKeyPartition.getPredicates();
				int tempSize = minTermList.size();
				for (int i = startIndex; i < tempSize; i++) {
					MinTerm oldMinTerm = minTermList.get(startIndex);
					minTermList.remove(startIndex);
					for (int j = 0; j < predicates.size(); j++) {
						MinTerm newMinTerm = new MinTerm(oldMinTerm,
								predicates.get(j));
						newMinTerm.setPos(++partitionCount + startIndex);
						minTermList.add(newMinTerm);
					}
					oldMinTerm = null;
				}
			}
		}

		return partitionCount;
	}

	public Integer getPartitionCount() {
		int count = 0;
		for (int i = 0; i < keyList.size(); i++) {
			String keyName = keyList.get(i);
			KeyPartition aKeyPartition = keyPartitions.get(keyName);
			count += aKeyPartition.getPartitionCount();
		}
		return count;
	}

	public void setKeyPartitionSize() {
		for (KeyPartition aKey : keyPartitions.values())
			aKey.setPartitionSize();
	}

	public void listPartitions(BufferedWriter out) throws IOException {
		for (KeyPartition aKeyPartition : keyPartitions.values()) {
			out.write(aKeyPartition.getName() + "\n");
			aKeyPartition.listPartitions(out);
		}
		System.out.println("");
	}

	public void updateKeyBound() {
		for (KeyPartition aKey : keyPartitions.values()) {
			if (aKey.getType() == 0)
				aKey.setBound();
		}
	}

	public void eliminateKey() {
		// eliminateUpdateAvg();

		elimateLowerThan(retainTopK);

		Set<String> keys = keyVisitMap.keySet();
		Set<String> rmKeys = new HashSet<String>();
		for (String aKey : keyPartitions.keySet()) {
			if (!keys.contains(aKey))
				rmKeys.add(aKey);
		}
		for (String rmKey : rmKeys)
			keyPartitions.remove(rmKey);
	}

	private void elimateLowerThan(int bound) {
		int keyNumber = keyVisitMap.size();
		if (keyNumber <= bound)
			return;

		HashMap<String, Integer> newKVMap = new HashMap<String, Integer>();
		for (String key : keyVisitMap.keySet()) {
			if (newKVMap.size() < bound)
				newKVMap.put(key, keyVisitMap.get(key));
			else {
				String eKey = null;
				int count = Integer.MAX_VALUE;
				for (String newKey : newKVMap.keySet()) {
					if (newKVMap.get(newKey) < count) {
						eKey = newKey;
						count = newKVMap.get(newKey);
					}
				}
				if (count < keyVisitMap.get(key)) {
					newKVMap.remove(eKey);
					newKVMap.put(key, keyVisitMap.get(key));
				}
			}
		}
		keyVisitMap = newKVMap;
	}

	private void eliminateUpdateAvg() {
		int totalVisit = 0;
		int keyNumber = keyVisitMap.size();

		for (Integer visitCount : keyVisitMap.values())
			totalVisit += visitCount;

		if (keyNumber == 0)
			return;

		int eliminateTH = totalVisit / keyNumber;
		List<String> eliminateKey = new ArrayList<String>();
		for (String aKey : keyVisitMap.keySet()) {
			if (keyVisitMap.get(aKey) < eliminateTH)
				eliminateKey.add(aKey);
		}

		for (int i = 0; i < eliminateKey.size(); i++) {
			keyVisitMap.remove(eliminateKey.get(i));
		}
	}

	public String getName() {
		return tableName;
	}

	public KeyPartition getKeyPartition(String key) {
		if (keyVisitMap.get(key) == null)
			keyVisitMap.put(key, 1);
		else
			keyVisitMap.put(key, keyVisitMap.get(key) + 1);

		return keyPartitions.get(key);
	}

}

/**
 * 
 * @author Xin Pan maintain partition result of a key
 * 
 * 
 */
class KeyPartition {
	private String key;
	//[tag xiaoyan] the list for all the available predicates for a key 
	private List<Predicate> predicates = new ArrayList<Predicate>();

	private int min;
	private int max;
	private int card;
	private int cnt; //the number of tuples in the table, xiaoyan
	private int type; //the type of this attribute, int or string

	private int minVisit = Integer.MAX_VALUE;
	private int maxVisit = Integer.MIN_VALUE;

	public KeyPartition(String key, int min, int max, int card, int cnt, int type) {
		this.key = key;
		this.min = min;
		this.max = max;
		this.card = card;
		this.cnt = cnt;
		this.type = type;

		Predicate aPredicate = new Predicate(Integer.MIN_VALUE / 2,
				Integer.MAX_VALUE / 2);
		predicates.add(aPredicate);
	}

	public void setPartitionSize() {
		for (int i = 0; i < predicates.size(); i++) {
			//[tag xiaoyan] for string value
			Predicate pred = predicates.get(i);
			if (pred.getType() != 0) {
				pred.setSelectivity(pred.getCard() / pred.getCount());
				pred.setCount(pred.getCount());
				//[tag xiaoyan] this.cnt == pred.getcount() ???
				continue;
			}
			int pMin = predicates.get(i).getMin();
			int pMax = predicates.get(i).getMax();
			//int estimateSize = (int) (((double) (pMax - pMin)) / (max - min) * card);
			//estimateSize = estimateSize == 0 ? 1 : estimateSize;
			//[tag xiaoyan] a new size estimation
			int estimateSize = (int)((double) (pMax - pMin) / (this.max - this.min) * this.cnt);
			if (estimateSize <= 0)
				estimateSize = 1;
			if (pMin == Integer.MIN_VALUE / 2 || pMax == Integer.MAX_VALUE / 2)
				estimateSize = 1;
			predicates.get(i).setSize(estimateSize);
			predicates.get(i).setCount(this.cnt);
			predicates.get(i).setSelectivity((double) (pMax - pMin) / (this.max - this.min));
			//System.out.println("pmax = " + pMax + ", pmin = " + pMin + ", this.max = " + this.max + ", this.min = " + this.min);
			//System.out.println("set sel = " + (double) (pMax - pMin) / (this.max - this.min) + ", cnt = " + this.cnt);
		}
	}

	public String getName() {
		return key;
	}
	
	public int getType() {
		return this.type;
	}

	public Integer getPartitionCount() {
		return predicates.size();
	}

	public boolean addPredicate(Predicate aPredicate) {
		//[tag xiaoyan] first handling when the predicate type is string
		int type = aPredicate.getType();
		if (type == 1) { // string type, only support eqaul operation now
			for (int i = 0; i < predicates.size(); i++) {
				if (predicates.get(i).getStrVal().compareTo(aPredicate.getStrVal()) == 0) {
					return false;
				}
			}
			predicates.add(aPredicate);
			return true;
		}
		
		updateBound(aPredicate);

		if (aPredicate.getMin() == null) {
			final int aMax = aPredicate.getMax();
			for (int i = 0; i < predicates.size(); i++) {
				if (predicates.get(i).getMax() == aMax)
					return false;
				else if (predicates.get(i).getMax() > aMax) {
					Predicate oldPredicate = predicates.get(i);

					aPredicate.setMin(oldPredicate.getMin());
					oldPredicate.setMin(aMax);
					predicates.add(i, aPredicate);

					return true;
				}
			}
		} else if (aPredicate.getMax() == null) {
			final int aMin = aPredicate.getMin();
			for (int i = 0; i < predicates.size(); i++) {
				if (predicates.get(i).getMin() == aMin)
					return false;
				else if (predicates.get(i).getMin() < aMin) {
					Predicate oldPredicate = predicates.get(i);

					aPredicate.setMin(oldPredicate.getMax());
					oldPredicate.setMax(aMin);
					predicates.add(i + 1, aPredicate);

					return true;
				}
			}
		} else {
			final int aMin = aPredicate.getMin();
			final int aMax = aPredicate.getMax();

			for (int i = 0; i < predicates.size(); i++) {
				Predicate oldPredicate = predicates.get(i);
				if (aMin + 1 <= oldPredicate.getMax()
						&& aMin - 1 >= oldPredicate.getMin()) {
					if (aMax < oldPredicate.getMax()) {
						Predicate first = new Predicate(aMin, aMax);
						Predicate second = new Predicate(aMax,
								oldPredicate.getMax());
						oldPredicate.setMax(aMin);

						predicates.add(i + 1, second);
						predicates.add(i + 1, first);

						return true;// it is important, avoid counting aMax
									// again
					} else {
						Predicate first = new Predicate(aMin,
								oldPredicate.getMax());
						oldPredicate.setMax(aMin);
						predicates.add(i + 1, first);
					}
				}
				if (aMax + 1 <= oldPredicate.getMax()
						&& aMax - 1 >= oldPredicate.getMin()) {
					Predicate first = new Predicate(oldPredicate.getMin(), aMax);
					oldPredicate.setMin(aMax);
					predicates.add(i, first);

					return true;
				}
			}
		}
		return false;
	}

	private void updateBound(Predicate aPredicate) {
		if (aPredicate.getMax() != null && aPredicate.getMax() > maxVisit)
			maxVisit = aPredicate.getMax();
		if (aPredicate.getMin() != null && aPredicate.getMin() < minVisit)
			minVisit = aPredicate.getMin();
	}

	public void listPartitions(BufferedWriter out) throws IOException {
		for (int i = 0; i < predicates.size(); i++) {
			Predicate aPredicate = predicates.get(i);
			out.write(aPredicate.getMin() + ":" + aPredicate.getMax() + "\t");
		}
		out.write("\t");
	}

	public List<Predicate> getPredicates() {
		return predicates;
	}

	public void setBound() {
		if (min > minVisit)
			min = minVisit;
		if (max < maxVisit)
			max = maxVisit;

		predicates.get(0).setMin(min);
		predicates.get(predicates.size() - 1).setMax(max);
		if (predicates.get(0).getMin() == predicates.get(0).getMax())
			predicates.remove(0);
		if (predicates.get(predicates.size() - 1).getMin() == predicates.get(predicates.size() - 1).getMax())
			predicates.remove(predicates.size() - 1);
	}
}