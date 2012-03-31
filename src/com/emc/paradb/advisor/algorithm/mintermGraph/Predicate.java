package com.emc.paradb.advisor.algorithm.mintermGraph;


public class Predicate
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
		
		if(sMin >= min && sMin < max ||
				sMax > min && sMax < max)
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