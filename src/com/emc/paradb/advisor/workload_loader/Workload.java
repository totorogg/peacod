package com.emc.paradb.advisor.workload_loader;

import java.util.HashMap;
import java.util.Vector;



public class Workload<T> extends Vector<T>
{
	private int transactionCount = 0;
	private HashMap<String, Integer> tableVisitMap = new HashMap<String, Integer>();

	
	public Workload()
	{
		super();
	}
	
	public boolean add(T tran)
	{
		transactionCount++;
		return super.add(tran);
	}
}

