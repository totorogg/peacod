package com.emc.paradb.advisor.algorithm.mintermGraphFile;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * 
 * @author Xin Pan
 *
 * a graph designed for minterm nodes
 */
public class Graph
{
	protected HashMap<String, Integer> startPos = null;
	protected HashMap<String, Integer> endPos = null;
	
	protected List<MinTerm> adjacencyList = null;
	protected HashMap<Integer, MinTerm> minTermIDMap = null;
	
	protected int part = 1;//partition into how many parts
	protected final int topK = 10;//divide how many nodes for repartitioning
	
	protected final String graphFileName = "minTermGraph";
	static int tranCnt = 0;
	
	public Graph(HashMap<String, Integer> tableStartPos, 
			HashMap<String, Integer> tableEndPos,
			List<MinTerm> adjacencyList)
	{
		this.startPos = tableStartPos;
		this.endPos = tableEndPos;
		this.adjacencyList = adjacencyList;
		
		tranCnt = 0;
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
	
	
	public void addConnect(Set<Integer> visitNodes) throws IOException
	{
		BufferedWriter out = new BufferedWriter(new FileWriter(GV.dirName+"/"+tranCnt++));
		for(Integer aNode : visitNodes)
		{
			out.write(String.valueOf(aNode));
			for(Integer aVisitNode : visitNodes)
				if(! aVisitNode.equals(aNode))
					out.write("\t" + aVisitNode+ "\t1");
			out.write("\n");
		}
		out.close();
	}
	
	public void initGraphFile() throws IOException
	{
		GraphFile.sortFile();
		GraphFile.multiWayMerge(GV.nWay);
		
		long start = System.currentTimeMillis();
		RandomAccessFile ranIn = new RandomAccessFile(GraphFile.getGraphFile(), "r");
		
		int i = 0;
		int pos = -1;
		long offset = 0;
		
		String line = null;
		
		//Iterator<MinTerm> iterator = adjacencyList.iterator();
		while((line = ranIn.readLine()) != null)
		{
			String[] splitLine = line.split("\t");
			
			pos = Integer.valueOf(splitLine[0]);
			
			int edgeCnt = (splitLine.length - 1) / 2;
			/*
			while(iterator.hasNext())
			{
				MinTerm aMT = iterator.next();
				if(pos == i)
				{
					aMT.setOffset(offset);
					aMT.setEdgeCnt(edgeCnt);
					offset = ranIn.getFilePointer();
					i++;
					break;
				}
				i++;
			}*/
			
			
			for(; i < adjacencyList.size(); i++)
			{
				if(pos == i)
				{
					adjacencyList.get(i).setOffset(offset);
					adjacencyList.get(i).setEdgeCnt(edgeCnt);
					offset = ranIn.getFilePointer();
					break;
				}
			}
		}
		ranIn.close();
		
		long end = System.currentTimeMillis();
		System.out.println("duration: " + (end - start));
	}
	
	public List<MinTerm> combine(String separator) throws IOException
	{
		List<MinTerm> toAddMT = new ArrayList<MinTerm>();
		
		int s = startPos.get(separator);
		int e = endPos.get(separator);		
		
		int rmCount = 0;
		int cmp = e - s;
		for(int j = 0; j < cmp; j++)
		{
			MinTerm aMinTerm = adjacencyList.get(s);
			if( !aMinTerm.hasNeighbor())
			{
				if (s + 1 >= adjacencyList.size() || !adjacencyList.get(s + 1).combinePre(aMinTerm))
				{
					if (s - 1 < 0 || !adjacencyList.get(s - 1).combineNext(aMinTerm)) 
					{
						System.err.println("Unable To Combine: " + s);
						toAddMT.add(aMinTerm);
						s++;
						continue;
					}
				}
				adjacencyList.remove(s);
				rmCount++;
			}
			else
				s++;
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
		
		return toAddMT;
	}
	
	public void partitionGraph(int part) throws Exception
	{
		this.part = part;
		
		HashMap<Integer, Integer> posMap = new HashMap<Integer, Integer>();
		int nodeSize = prepareForMETIS(posMap);
		
		int edgeCount = 0;
		for(int i = 0; i < adjacencyList.size(); i++)
			edgeCount += adjacencyList.get(i).getEdgeNumber();
			
		if(edgeCount % 2 != 0)
			System.err.println("Error edge count");
		edgeCount /= 2;
		
		BufferedWriter out = new BufferedWriter(new FileWriter(graphFileName));
		RandomAccessFile ranIn = new RandomAccessFile(GraphFile.getGraphFile(), "r");
		out.write(nodeSize + " " + edgeCount + " 011\n");
		
		for(int i = 0; i < adjacencyList.size(); i++)
		{
			
			if(!adjacencyList.get(i).hasNeighbor())
				continue;
			
			out.write(adjacencyList.get(i).getEstimatedSize() + " ");
			
			HashMap<Integer, Integer> neighbor = adjacencyList.get(i).getNeighbor(ranIn);
			for (Integer id : neighbor.keySet())
			{
				if(posMap.get(id) == null)
				{
					System.err.println("no such pos map");
					continue;
				}
				out.write(posMap.get(id) + " " + neighbor.get(id) + " ");
			}
			out.write("\n");
		}
		out.close();
		
		Process p = Runtime.getRuntime().exec("gpmetis " + 
												graphFileName + " " + part);
		p.waitFor();
	
		
		FileReader instream = new FileReader(graphFileName + ".part." + part);
		BufferedReader in = new BufferedReader(instream);
		
		Random r = new Random(part);
		for(int i = 0; i < adjacencyList.size(); i++)
		{
			if(!adjacencyList.get(i).hasNeighbor())
				adjacencyList.get(i).setNode(r.nextInt());
			else
			{
				int node = Integer.valueOf(in.readLine());
				adjacencyList.get(i).setNode(node);
			}
		}
		in.close();
	}
	
	protected int prepareForMETIS(HashMap<Integer, Integer> posMap)
	{
		minTermIDMap = new HashMap<Integer, MinTerm>();

		int pos = 1;
		
		for(int i = 0; i < adjacencyList.size(); i++)
		{	
			if(adjacencyList.get(i).hasNeighbor())
			{
				posMap.put(adjacencyList.get(i).getPos()-1, pos);
				adjacencyList.get(i).setPos(pos);
				minTermIDMap.put(pos, adjacencyList.get(i));
				pos++;
			}
		}
		
		int pos2 = pos;
		for(int i = 0; i < adjacencyList.size(); i++)
		{
			if(!adjacencyList.get(i).hasNeighbor())
			{
				adjacencyList.get(i).setPos(pos2);
				minTermIDMap.put(pos2, adjacencyList.get(i));
				pos2++;
			}
		}
		
		
		return pos-1;
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
	/*
	public void refinePartition() throws Exception
	{
		divideGraph();
		
		boolean prepare = prepareParRefine();
		
		Process p = Runtime.getRuntime().exec("parmetis " +  graphFileName + " 2 " + part +
											  " 1 1 1 1 ");
		p.waitFor();
		
		FileReader instream = new FileReader(graphFileName + ".part");	
		BufferedReader in = new BufferedReader(instream);

		for (int i = 0; i < adjacencyList.size(); i++) 
		{
			int node = Integer.valueOf(in.readLine());
			adjacencyList.get(i).setNode(node);
		}
		in.close();
	}*/
	/*
	protected boolean prepareParRefine()
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
	*/
	/*
	protected void divideGraph()
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
	*/
	/**
	 * divide minTerm into two parts equally
	 * @param dMinTerm
	 */
	/*
	protected void divideNode(MinTerm dMinTerm, List<MinTerm> subAdjList)
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
	}*/
	
	
	public List<Integer> searchPlacement(List<Predicate> searchMT, String separator)
	{
		List<Integer> visitParts = new ArrayList<Integer>();
		
		Set<Integer> visitNodes = this.match(searchMT, separator);
		for(Integer nodeID : visitNodes)
			visitParts.add(adjacencyList.get(nodeID).getNode());
		

		
		return visitParts;
	}
}