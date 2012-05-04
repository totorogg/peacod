package com.emc.paradb.advisor.workload_loader;

import java.util.HashMap;
import java.util.Vector;


/**
 * workload object extends from vector
 * generic type T is used for transaction object
 * 
 * @author XPan
 *
 * @param <T>
 */
public class Workload<T> extends Vector<T>
{
	//record the transaction number we have
	private int transactionCount = 0;
	//not used
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

