package com.emc.paradb.advisor.algorithm.mintermGraph;


import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;


public class MinTerm
{
	List<Predicate> terms = null;
	int node = 0;
	int pos = -1;
	HashMap<Integer, Integer> edgeCount = new HashMap<Integer, Integer>();
	
	public MinTerm(MinTerm oldMinTerm, Predicate newPredicate)
	{
		terms = new ArrayList<Predicate>();
		
		List<Predicate> oldTerms = oldMinTerm.getTerms();
		for(int i = 0; i < oldTerms.size(); i++)
			terms.add(new Predicate(oldTerms.get(i)));
		
		terms.add(new Predicate(newPredicate));
	}
	
	public MinTerm(List<Predicate> terms)
	{
		this.terms = terms;
	}
	
	public int getEstimatedSize()
	{
		int size = 0;
		for(int i = 0; i < terms.size(); i++)
		{
			int gap = (int)Math.sqrt((double)terms.get(i).getMax() - terms.get(i).getMin());
			if(gap > Integer.MAX_VALUE / 5 || gap <= 0)
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
		if(pos == -1)
			System.err.println("pos not inited");
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
	
	public int getEdgeNumber()
	{
		int edge = 0;
		for(Integer count : edgeCount.values())
			edge += count;
		
		return edge;
	}
}




class SizeComparator implements Comparator<MinTerm>
{

	@Override
	public int compare(MinTerm arg0, MinTerm arg1) {
		// TODO Auto-generated method stub
		int size1 = arg0.getEstimatedSize();
		int size2 = arg0.getEstimatedSize();
		
		return size1 > size2 ? -1 : (size1 == size2 ? 0 : 1);
	}
}