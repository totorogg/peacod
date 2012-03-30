package com.emc.paradb.advisor.algorithm.mintermGraph;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 
 * @author Xin Pan
 *
 * a graph designed for minterm nodes
 */
public class Graph
{
	private HashMap<String, Integer> startPos = null;
	private HashMap<String, Integer> endPos = null;
	
	private List<MinTerm> adjacencyList = null;
	private HashMap<Integer, MinTerm> minTermIDMap = null;
	
	private int part = 1;//partition into how many parts
	private final int topK = 10;//divide how many nodes for repartitioning
	
	private final String graphFileName = "minTermGraph";

	public Graph(HashMap<String, Integer> tableStartPos, 
			HashMap<String, Integer> tableEndPos,
			List<MinTerm> adjacencyList)
	{
		this.startPos = tableStartPos;
		this.endPos = tableEndPos;
		this.adjacencyList = adjacencyList;
	}
	
	public Set<Integer> match(List<Predicate> searchMT, String separator)
	{
		Set<Integer> matchSet = new HashSet<Integer>();
		
		int start = startPos.get(separator);
		int end = endPos.get(separator);
		for(int i = start; i < end; i++)
		{
			if(adjacencyList.get(i).match(searchMT))
				matchSet.add(i);
		}
		
		return matchSet;
	}
	
	public void addConnect(Set<Integer> visitNodes)
	{
		for(Integer aNode : visitNodes)
			for(Integer aVisitNode : visitNodes)
				if(! aVisitNode.equals(aNode))
					adjacencyList.get(aNode).addConnect(adjacencyList.get(aVisitNode).getPos());
	}
	
	
	public void combine(String separator)
	{
		int s = startPos.get(separator);
		int e = endPos.get(separator);		
		
		//find a minterm node whose has neighbour
		int lastValidMT = 0;
		int validPos = 0;
		for(int i = 1; i < adjacencyList.size(); i++)
		{
			if(adjacencyList.get(i).getNeibourSize() != 0)
			{
				lastValidMT = adjacencyList.get(i).getPos();
				validPos = i;
				break;
			}
		}

		int rmCount = 0;
		int cmp = e - s;
		for(int j = 0; j < cmp; j++)
		{
			MinTerm aMinTerm = adjacencyList.get(s);
			if(aMinTerm.getNeibourSize() == 0)
			{
				if (s + 1 >= adjacencyList.size() || !adjacencyList.get(s + 1).combinePre(aMinTerm))
				{
					if (s - 1 < 0 || !adjacencyList.get(s - 1).combineNext(aMinTerm)) 
					{
						aMinTerm.addConnect(lastValidMT);
						adjacencyList.get(validPos).addConnect(aMinTerm.getPos());
						System.err.println("Unable To Combine: " + s);
						s++;
						continue;
					}
				}
				adjacencyList.remove(s);
				rmCount++;
			}
			else
			{
				lastValidMT = adjacencyList.get(s).getPos();
				validPos = s;
				s++;
			}
		}
			
		//update separator position map
		endPos.put(separator, endPos.get(separator) - rmCount);
		
		for(String aSepa : startPos.keySet())
		{
			if(startPos.get(aSepa) > s)
			{
				startPos.put(aSepa, startPos.get(aSepa) - rmCount);
				endPos.put(aSepa, endPos.get(aSepa) - rmCount);
			}
		}
	}
	
	public void partitionGraph(int part) throws Exception
	{
		this.part = part;
		
		int edgeCount = prepareForMETIS();
		
		if(edgeCount % 2 != 0)
			System.err.println("Error edge count");
		edgeCount /= 2;
		
			
		FileWriter fstream = new FileWriter(graphFileName);
		BufferedWriter out = new BufferedWriter(fstream);
		
		out.write(adjacencyList.size() + " " + edgeCount + " 011\n");
		
		for(int i = 0; i < adjacencyList.size(); i++)
		{
			out.write(adjacencyList.get(i).getEstimatedSize() + " ");

			HashMap<Integer, Integer> neighbourMap = adjacencyList.get(i).getNeighbour();
			for (Integer neighbourID : neighbourMap.keySet())
				out.write(neighbourID + " " + neighbourMap.get(neighbourID) + " ");
			out.write("\n");
		}
		out.close();
		
		Process p = Runtime.getRuntime().exec("gpmetis " + 
												graphFileName + " " + part);
		p.waitFor();
	
		
		FileReader instream = new FileReader(graphFileName + ".part." + part);
		BufferedReader in = new BufferedReader(instream);
		
		for(int i = 0; i < adjacencyList.size(); i++)
		{
			int node = Integer.valueOf(in.readLine());
			adjacencyList.get(i).setNode(node);
		}
		in.close();
	}
	
	private int prepareForMETIS()
	{
		int edgeCount = 0;
		HashMap<Integer, Integer> posMap = new HashMap<Integer, Integer>();
		minTermIDMap = new HashMap<Integer, MinTerm>();
		
		for(int i = 0; i < adjacencyList.size(); i++)
		{
			posMap.put(adjacencyList.get(i).getPos(), i+1);
			adjacencyList.get(i).setPos(i+1);
			minTermIDMap.put(i+1, adjacencyList.get(i));
		}
		
		for(int i = 0; i < adjacencyList.size(); i++)
			edgeCount += adjacencyList.get(i).renewPos(posMap);
		
		return edgeCount;
	}
	
	public void display() throws IOException
	{
		//display the placement strategy, for validation
		BufferedWriter out = new BufferedWriter(new FileWriter("placement"));
		for(int i = 0; i < part; i++)
		{
			for(int j = 0; j < adjacencyList.size(); j++)
			{
				if(adjacencyList.get(j).getNode() == i)
					out.write(adjacencyList.get(j).toString() + "&&");
			}
			out.write("\n*******************************\n");
		}
		out.close();
	}
	
	public void refinePartition() throws Exception
	{
		divideGraph();
		
		boolean prepare = prepareParRefine();
		
		Process p = Runtime.getRuntime().exec("parmetis " +  graphFileName + " 2 " + part +
											 " 1 1 1 1 ");
		p.waitFor();
		
		FileReader instream = new FileReader(graphFileName + ".part");	
		BufferedReader in = new BufferedReader(instream);

		for (int i = 0; i < adjacencyList.size(); i++) {
			int node = Integer.valueOf(in.readLine());
			adjacencyList.get(i).setNode(node);
		}
		in.close();
	}
	
	private boolean prepareParRefine()
	{
		int edgeCount = 0;
		HashMap<Integer, Integer> posMap = new HashMap<Integer, Integer>();
		minTermIDMap.clear();
		
		//renew adjacencyList ID
		for(int i = 0; i < adjacencyList.size(); i++)
		{
			posMap.put(adjacencyList.get(i).getPos(), i+1);
			adjacencyList.get(i).setPos(i+1);
			minTermIDMap.put(i+1, adjacencyList.get(i));
		}
		for(int i = 0; i < adjacencyList.size(); i++)
			edgeCount += adjacencyList.get(i).renewPos(posMap);
		
		try 
		{
			//write out the modified partition result
			BufferedWriter partFile = new BufferedWriter(new FileWriter(graphFileName + ".part"));
			for(int i = 0; i < adjacencyList.size(); i++)
			{
				partFile.write(adjacencyList.get(i).getNode() + "\n");
				edgeCount += adjacencyList.get(i).getEdgeNumber();
			}
			partFile.close();
			
			//check if the edge number is even
			if(edgeCount % 2 != 0)
			{
				System.err.println("Error edge count");
				return false;
			}
			edgeCount /= 2;
			
			
			//write out the graph file
			BufferedWriter graphFile = new BufferedWriter(new FileWriter(graphFileName));
			graphFile.write(adjacencyList.size() + " " + edgeCount + " 011\n");
			
			for(int i = 0; i < adjacencyList.size(); i++)
			{
				graphFile.write(adjacencyList.get(i).getEstimatedSize() + " ");

				HashMap<Integer, Integer> neighbourMap = adjacencyList.get(i).getNeighbour();
				for (Integer neighbourID : neighbourMap.keySet())
					graphFile.write(neighbourID + " " + neighbourMap.get(neighbourID) + " ");
				graphFile.write("\n");
			}
			graphFile.close();
			
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return true;
	}
	
	private void divideGraph()
	{
		//sort minterm from max node size to min size
		HashMap<String, List<MinTerm>> sepaListMap = new HashMap<String, List<MinTerm>>();
		
		for(String aSepa : startPos.keySet())
		{
			int start = startPos.get(aSepa);
			int end = endPos.get(aSepa);
			
			List<MinTerm> subAdjList = 
					new ArrayList<MinTerm>(adjacencyList.subList(start, end));
			sepaListMap.put(aSepa, subAdjList);
			
			Collections.sort(subAdjList, new SizeComparator());
			Collections.reverse(subAdjList);
			
			//divide every topK minTerm into two parts equally
			for(int i = 0; i < subAdjList.size() && i < topK; i++)
			{
				MinTerm divideMT = subAdjList.get(i);
				divideNode(divideMT, subAdjList);
			}
		}
		
		//update the separator position map
		startPos.clear();
		endPos.clear();
		adjacencyList.clear();
		int start = 0;
		for(String aSepa : sepaListMap.keySet())
		{
			List<MinTerm> subAdjList = sepaListMap.get(aSepa);
			adjacencyList.addAll(subAdjList);
			startPos.put(aSepa, start);
			endPos.put(aSepa, start + subAdjList.size());
			
			start += subAdjList.size();
		}

	}
	
	/**
	 * divide minTerm into two parts equally
	 * @param dMinTerm
	 */
	private void divideNode(MinTerm dMinTerm, List<MinTerm> subAdjList)
	{
		//find large predicate, and divide the minTerm over it
		List<Predicate> terms = dMinTerm.getTerms();
		int largestRange = terms.get(0).getMax() - terms.get(0).getMin();
		int largestPos = 0;
		for(int i = 1; i < terms.size(); i++)
		{
			if(terms.get(i).getMax() - terms.get(i).getMin() > largestRange)
			{
				largestRange = terms.get(i).getMax() - terms.get(i).getMin();
				largestPos = i;
			}
		}
		if(largestRange <= 1)
			return;
		
		//create the new minterm divided from the old one
		List<Predicate> newTerms = new ArrayList<Predicate>();
		for(int i = 0; i < terms.size(); i++)
		{
			if(i == largestPos)
			{
				int newMin = terms.get(largestPos).getMin() + largestRange/2;
				Predicate newPredicate = new Predicate(newMin, terms.get(largestPos).getMax());
				newTerms.add(newPredicate);
				
				terms.get(largestPos).setMax(newMin);
			}
			else
			{
				Predicate newPredicate = new Predicate(terms.get(i).getMin(), terms.get(i).getMax());
				newTerms.add(newPredicate);
			}
		}
		
		MinTerm newMinTerm = new MinTerm(newTerms);
		int newMinTermID = adjacencyList.size() + 1;
		newMinTerm.setPos(newMinTermID);
		newMinTerm.setNode(dMinTerm.getNode());
		
		//add the new minTerm to list and hash map
		subAdjList.add(newMinTerm);
		adjacencyList.add(newMinTerm);//add this to increase ID number
		minTermIDMap.put(newMinTermID, newMinTerm);
		
		
		HashMap<Integer, Integer> neighbours = dMinTerm.getNeighbour();
		HashMap<Integer, Integer> newNei = newMinTerm.getNeighbour();
		for(Integer neighbourID : neighbours.keySet())
		{
			newNei.put(neighbourID, neighbours.get(neighbourID));
			
			MinTerm neighbourT = minTermIDMap.get(neighbourID);
			neighbourT.getNeighbour().put(newMinTermID, neighbours.get(neighbourID));
		}
		dMinTerm.addConnect(newMinTermID);
		newMinTerm.addConnect(dMinTerm.getPos());
	}
	
	
	public List<Integer> searchPlacement(List<Predicate> searchMT, String separator)
	{
		List<Integer> visitParts = new ArrayList<Integer>();
		
		Set<Integer> visitNodes = this.match(searchMT, separator);
		for(Integer nodeID : visitNodes)
			visitParts.add(adjacencyList.get(nodeID).getNode());
		
		return visitParts;
	}
}