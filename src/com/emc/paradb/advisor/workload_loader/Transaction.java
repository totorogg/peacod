package com.emc.paradb.advisor.workload_loader;

import java.util.Vector;

/**
 * a transaction class, extending from vector
 * workload class contrains a list of transaction object
 * sql statements are stored as a list in the transaction object
 * 
 * @author xpan
 *
 * @param <T>
 */
public class Transaction<T> extends Vector<T>
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int statementCount = 0;
	private int selectCount = 0;
	private int updateCount = 0;
	private int insertCount = 0;
	private int deleteCount = 0;
	
	public Transaction()
	{
		super();
	}
	
	public boolean add(T statement)
	{
		statementCount++;
		if(statement instanceof SelectAnalysisInfo)
			selectCount++;
		else if(statement instanceof UpdateAnalysisInfo)
			updateCount++;
		else if(statement instanceof InsertAnalysisInfo)
			insertCount++;
		else if(statement instanceof DeleteAnalysisInfo)
			deleteCount++;
		
		return super.add(statement);
	}
	
}