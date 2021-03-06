package com.emc.paradb.advisor.algorithm.hypergraphpartitioning;


public class Predicate
{
	private Integer min = null;
	private Integer max = null;
	private Integer size = null;
	//[tag xiaoyan] selectivity and cnt is used to estimate the #tuples of each predicate
	private double selectivity = 0.0;
	private int cnt = 0;
	//[tag xiaoyan] string type of value, only support equal
	private String strval = "";
	//[tag xiaoyan] 0 is for integer predicate, 1 is for string predicate
	private int type = 0;
	
	public Predicate()
	{}
	
	public Predicate(int min, int max)
	{
		this.min = min;
		this.max = max;
		//size = 1;
	}
	public Predicate(String strval)
	{
		this.strval = strval;
		this.type = 1;
	}
	
	public Predicate(Predicate copyP)
	{
		min = copyP.min;
		max = copyP.max;
		size = copyP.size;
		this.selectivity = copyP.selectivity;
		this.cnt = copyP.cnt;
		this.strval = copyP.strval;
	}
	public Integer getSize()
	{
		if(size == null)
			System.err.println("Err: predicate has no size");
		return size;
	}
	
	public void setSize(int size)
	{
		this.size = size;
	}
	
	public double getSelectivity() {
		return this.selectivity;
	}
	
	public void setSelectivity(double selectivity) {
		this.selectivity = selectivity;
		System.out.println("set sel = " + this.selectivity);
	}
	
	public int getCount() {
		return this.cnt;
	}
	
	public void setCount(int cnt) {
		this.cnt = cnt;
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
	
	public String getStrVal() {
		return this.strval;
	}
	
	public void setStrVal(String strval) {
		this.strval = strval;
	}
	
	public boolean match(Predicate searchP)
	{
		int sMin = searchP.getMin();
		int sMax = searchP.getMax();
		
		//Note that, we add this condition manually
		if(sMin >= sMax)
			return true;
		
		if(sMin >= min && sMin < max ||
				sMax > min && sMax < max ||
					sMin < min && sMax >= max)
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
		return min +":"+ max + ":" + size;
	}
}