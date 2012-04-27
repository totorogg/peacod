package com.emc.paradb.advisor.algorithm.finegrainedgraphparititioning;


import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;


public class MinTerm
{
	protected List<Predicate> terms = null;
	protected int node = 0;
	protected int pos = -1;
	
	protected int size = 0;
	
	protected long offset = -1;
	protected long length = -1;
	
	protected int edgeCnt = 0;
	
	public MinTerm(MinTerm oldMinTerm, Predicate newPredicate)
	{
		terms = new ArrayList<Predicate>();
		
		List<Predicate> oldTerms = oldMinTerm.getTerms();
		for(int i = 0; i < oldTerms.size(); i++)
			terms.add(new Predicate(oldTerms.get(i)));
		
		terms.add(new Predicate(newPredicate));
		
	}
	
	public MinTerm(Predicate aPredicate)
	{
		terms = new ArrayList<Predicate>();
		terms.add(aPredicate);
	}
	
	
	public int getEstimatedSize()
	{
		int size = Integer.MAX_VALUE;
		for(int i = 0; i < terms.size(); i++)
		{
			//size += terms.get(i).getSize();
			//[tag by xiaoyan] using the min of each size?
			size = Math.min(size, terms.get(i).getSize());
		}
		return size;
	}
	
	public void setNode(int node)
	{
		this.node = node;
	}
	public void setOffset(long offset)
	{
		this.offset = offset;
	}
	public void setEdgeCnt(int edgeCnt)
	{
		this.edgeCnt = edgeCnt;
	}
	
	public int getNode()
	{
		return node;
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
	
	public boolean hasNeighbor()
	{
		if(offset != -1)
			return true;
		return false;
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

	
	

	
	public List<Predicate> getTerms()
	{
		return terms;
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
		return edgeCnt;
	}
	
	public HashMap<Integer, Integer> getNeighbor(RandomAccessFile in) throws IOException
	{
		if(!hasNeighbor())
			return null;
		
		HashMap<Integer, Integer> neighbor = new HashMap<Integer, Integer>();
		in.seek(offset);
		String line = in.readLine();
		if(line == null)
		{
			System.err.println("Err: no such node");
			return null;
		}
		
		String[] splitLine = line.split("\t");
		for(int i = 1; i < splitLine.length; i+=2)
		{
			Integer neiPos = Integer.valueOf(splitLine[i]);
			Integer weight = Integer.valueOf(splitLine[i+1]);
			neighbor.put(neiPos, weight);
		}
		return neighbor;
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