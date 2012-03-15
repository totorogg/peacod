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
import java.util.Set;

import com.emc.paradb.advisor.data_loader.DBData;
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
 * This algorithm select the primary key as partition key
 * Each node is a range of a partition key.
 * Nodes are then partitioned by graph partition algorithm
 * 
 * @author Xin Pan
 *
 */
public class RangeGraph implements PlugInterface
{
	Connection conn = null;
	Workload<Transaction<Object>> workload = null;
	DBData dbData = null;
	int nodes = 0;
	int graphNodeCount = 0;
	int graphEdgeCount = 0;
	HashBased hash = null;
	HashMap<String, List<String>> tableKeyMap = null;
	HashMap<String, List<GraphNode>> graph = null;
	
	//temp
	int temp1 = 0;
	int temp2 = 0;
	
	@Override
	public boolean accept(Connection conn, Workload<Transaction<Object>> workload,
			DBData dbData, int nodes) 
	{
		this.conn = conn;
		this.workload = workload;
		this.dbData = dbData;
		this.nodes = nodes;
		graphNodeCount = 0;
		graphEdgeCount = 0;
		temp1 = temp2 = 0;
		tableKeyMap = new HashMap<String, List<String>>();
		graph = new HashMap<String, List<GraphNode>>();
		
		try
		{
			setPartition();
			hash = new HashBased(nodes);
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		return true;
	}

	private void setPartition() throws Exception
	{
		setTableKeyMap();
		initGraph();
		
		for(Transaction<Object> aTran : workload)
			updateEdge(aTran);
		
		partitionGraph(graph);
	}
	
	private void partitionGraph(HashMap<String, List<GraphNode>> graph) throws Exception
	{
		List<GraphNode> sortedNodes = sortGraph();
		String graphFile = "graph";
			
		FileWriter fstream = new FileWriter(graphFile);
		BufferedWriter out = new BufferedWriter(fstream);
		
		out.write(graphNodeCount + " " + graphEdgeCount + " 001\n");
		
		for(int i = 0; i < sortedNodes.size(); i++)
		{
			HashMap<GraphNode, Integer> edgeWeightMap = sortedNodes.get(i).getGraphEdgeWeightMap();
			for (GraphNode adjNode : edgeWeightMap.keySet())
				out.write(adjNode.getID() + " " + edgeWeightMap.get(adjNode).intValue() + " ");
			out.write("\n");
		}
		out.close();
		
		Process p = Runtime.getRuntime().exec("gpmetis " + graphFile + " " + nodes);
		p.waitFor();
		
		FileReader instream = new FileReader(graphFile + ".part." + nodes);
		BufferedReader in = new BufferedReader(instream);
		
		for(int i = 0; i < sortedNodes.size(); i++)
		{
			int node = Integer.valueOf(in.readLine());
			sortedNodes.get(i).setPlacement(node);
		}
		in.close();
	}
	
	private List<GraphNode> sortGraph()
	{
		List<GraphNode> sortedNodes = new ArrayList<GraphNode>();
		
		for(List<GraphNode> nodeList : graph.values())
		{
			for(GraphNode aNode : nodeList)
			{
				if(aNode.getEdgeSize() > 0)
				{
					graphEdgeCount += aNode.getEdgeSize();
					aNode.setID(++graphNodeCount);
					sortedNodes.add(aNode);
				}
			}
		}
		if(graphEdgeCount % 2 != 0)
		{
			System.err.println("Error graph Edge Count");
		}
		graphEdgeCount /= 2;
		
		return sortedNodes;
	}
	
	private void updateEdge(Transaction<Object> aTran)
	{
		Set<GraphNode> visitedNodes = new HashSet<GraphNode>();
		
		for(Object statement : aTran)
		{
			if(statement instanceof SelectAnalysisInfo)
			{
				SelectAnalysisInfo select = (SelectAnalysisInfo)statement;
				
				for(WhereKey key : select.getWhereKeys())
				{						
					String tableName = key.getTableName();
					if(tableKeyMap.get(tableName).get(0).equals(key.getKeyName()))
					{
						int value = Integer.valueOf(key.getKeyValue());
						updateVisited(key.getKeyName(), value, visitedNodes);
					}
				}
			}
			else if(statement instanceof UpdateAnalysisInfo)
			{
				UpdateAnalysisInfo update = (UpdateAnalysisInfo)statement;
				for(WhereKey key : update.getWhereKeys())
				{
					String tableName = key.getTableName();
					if(tableKeyMap.get(tableName).get(0).equals(key.getKeyName()))
					{
						int value = Integer.valueOf(key.getKeyValue());
						updateVisited(key.getKeyName(), value, visitedNodes);
					}
				}
			}
			else if(statement instanceof InsertAnalysisInfo)
			{
				InsertAnalysisInfo insert = (InsertAnalysisInfo)statement;
				
				for(String key : insert.getKeyValueMap().keySet())
				{
					String tableName = insert.getTable();
					if(tableKeyMap.get(tableName).get(0).equals(key))
					{
						int value = Integer.valueOf(insert.getKeyValueMap().get(key));
						updateVisited(key, value, visitedNodes);
					}
				}
			}
			else if(statement instanceof DeleteAnalysisInfo)
			{
				DeleteAnalysisInfo delete = (DeleteAnalysisInfo)statement;
				for(WhereKey key : delete.getWhereKeys())
				{
					String tableName = key.getTableName();
					if(tableKeyMap.get(tableName).get(0).equals(key.getKeyName()))
					{
						int value = Integer.valueOf(key.getKeyValue());
						updateVisited(key.getKeyName(), value, visitedNodes);
					}
				}
			}
		}
		
		for(GraphNode aNode : visitedNodes)
			for(GraphNode aVisitedNode : visitedNodes)
				if(!aNode.equals(aVisitedNode))
					aNode.addVisited(aVisitedNode);
	}
	
	private void updateVisited(String key, int value, Set<GraphNode> visitedNodes)
	{
		List<GraphNode> nodeList = graph.get(key);
		
		for(GraphNode aNode : nodeList)
		{
			if(aNode.match(value, key))
			{
				visitedNodes.add(aNode);
				break;
			}
		}
	}
	
	private void initGraph()
	{
		for(String tableName : tableKeyMap.keySet())
		{
			Statement stmt;
			String key = tableKeyMap.get(tableName).get(0);
			
			try 
			{
				stmt = conn.createStatement();
				ResultSet resultMin = stmt.executeQuery("select min("+key+") from "+
										QueryPrepare.prepare(tableName)+";");
				
				int type = resultMin.getMetaData().getColumnType(1);
				int min = 0;
				if(type == Types.INTEGER)
				{	
					resultMin.next();
					min = resultMin.getInt(1);
				}
				else
				{
					System.err.println(key + " is not integer type");
					System.exit(-1);
				}
				
				ResultSet resultMax = stmt.executeQuery("select max("+key+") from "+
										QueryPrepare.prepare(tableName)+";");
				resultMax.next();
				int max = resultMax.getInt(1);

				int interval = (max - min + 1) / nodes;
				interval = (interval == 0) ? 1 : (int)Math.sqrt((double)interval);
				
				for (int i = min; i <= max; i += interval) 
				{
					GraphNode aNode = new GraphNode(i, i + interval, key);
					
					if (graph.get(key) == null) 
					{
						List<GraphNode> nodeList = new ArrayList<GraphNode>();
						nodeList.add(aNode);
						graph.put(key, nodeList);
					} 
					else
						graph.get(key).add(aNode);
				}
			} 
			catch (SQLException e) 
			{
				e.printStackTrace();
			}
		}

	}

	private void setTableKeyMap()
	{
		HashMap<String, TableNode> tableMap = dbData.getMetaData();
		for(String table : tableMap.keySet())
		{
			TableNode tableNode = tableMap.get(table);
			List<String> primaryKey = tableNode.getPrimaryKey();
			List<String> keys = new ArrayList<String>();
			if(primaryKey.size() > 0)
				keys.add(primaryKey.get(0));
			else
				keys.add(tableNode.getAttrVector().get(0).getName());
			
			tableKeyMap.put(table, keys);
		}
	}
	
	@Override
	public HashMap<String, List<String>> getPartitionKey()
	{
		return tableKeyMap;
	}

	@Override
	public List<Integer> getNode(List<KeyValuePair> kvPairs) 
	{
		List<Integer> nodes = new ArrayList<Integer>();
		if(kvPairs.get(0).getRange() != Range.EQUAL)
		{
			nodes.add(-1);
			return nodes;
		}
		
		String key = kvPairs.get(0).getKey();
		String value = kvPairs.get(0).getValue();	
		
		if(value != null)
		{
			List<GraphNode> nodeList = graph.get(key);
			int node = -1;
			for(GraphNode aNode : nodeList)
			{
				if(aNode.match(Integer.valueOf(value), key))
				{
					node = aNode.getPlacement();
					break;
				}
			}
			if(node == -1)
			{
				nodes.add(hash.getPlacement(value));
				temp1++;
			}
			else
			{
				temp2++;
				nodes.add(node);
			}
		}
		else
			nodes.add(-1);
		
		return nodes;
	}

	@Override
	public List<String[]> getSetting() {
		// TODO Auto-generated method stub
		return null;
	}
	
}

class GraphNode
{
	private String key = null;
	private int id = -1;
	private int low;
	private int max;
	private int node = -1;
	
	private HashMap<GraphNode, Integer> edgeWeightMap = new HashMap<GraphNode, Integer>();
	
	public GraphNode(int low, int max, String key)
	{
		this.low = low;
		this.max = max;
		this.key = key;
	}
	
	public int getEdgeSize()
	{
		return edgeWeightMap.size();
	}
	
	public void setID(int i)
	{
		id = i;
	}
	public int getID()
	{
		return id;
	}
	
	public boolean match(int value, String key)
	{
		if(value >= low && value < max && this.key.equals(key))
			return true;
		else
			return false;
	}
	
	public void setPlacement(int node)
	{
		this.node = node;
	}
	public int getPlacement()
	{
		return node;
	}
	
	public void addVisited(GraphNode visitedNode)
	{
		if(edgeWeightMap.get(visitedNode) == null)
			edgeWeightMap.put(visitedNode, 1);
		else
			edgeWeightMap.put(visitedNode, edgeWeightMap.get(visitedNode) + 1);
	}
	
	public HashMap<GraphNode, Integer> getGraphEdgeWeightMap()
	{
		return edgeWeightMap;
	}

	public boolean equals(Object obj)
	{
		if(obj instanceof GraphNode)
		{
			GraphNode aNode = (GraphNode)obj;
			if(aNode.toString().equals(this.toString()))
				return true;
			else
				return false;
		}
		
		return false;
	}
	
	public String toString()
	{
		return key + ":" + low + ":" + max + ":" + node;
	}
}