package com.emc.paradb.advisor.workload_loader;

import java.util.Vector;



public class Workload<T> extends Vector<T>
{
	private int transactionCount = 0;
	
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

