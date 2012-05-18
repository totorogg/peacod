package com.emc.paradb.advisor.algorithm.rewrite;


public class Predicate
{
	private Integer min = null;
	private Integer max = null;
	private Integer size = null;
	//[tag xiaoyan] selectivity and cnt is used to estimate the #tuples of each predicate
	private double selectivity = 0.0;
	private int cnt = 1;
	private int card = 0;
	//[tag xiaoyan] string type of value, only support equal
	private String strval = "";
	//[tag xiaoyan] 0 is for integer predicate, 1 is for string predicate
	private int type = 0;
	private int op = 0; //operation for this predicate, 0 is for equal
	private String colname;
	
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
	
	public Predicate(int min, int max, String colname)
	{
		this.min = min;
		this.max = max;
		this.colname = colname;
		//size = 1;
	}
	public Predicate(String strval, String colname)
	{
		this.strval = strval;
		this.type = 1;
		this.colname = colname;
	}
	
	public Predicate(Predicate copyP)
	{
		this.min = copyP.min;
		this.max = copyP.max;
		this.size = copyP.size;
		this.selectivity = copyP.selectivity;
		this.cnt = copyP.cnt;
		this.card = copyP.card;
		this.strval = copyP.strval;
		this.type = copyP.type;
		this.colname = copyP.colname;
	}
	
	public void setType(int type) {
		this.type = type;
	}
	
	public int getType() {
		return this.type;
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
	
	public int getCard() {
		return this.card;
	}
	
	public void setCard(int card) {
		this.card = card;
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
		//[tag xiaoyan] string value
		if (this.type != 0 || searchP.getType() != 0) {
			String strval = searchP.getStrVal();
			if (this.strval.compareTo(strval) == 0) {
				return true;
			} else {
				return false;
			}
		}
		//[tag xiaoyan] follow is the integer value
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