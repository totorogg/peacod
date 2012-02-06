package com.emc.paradb.advisor.controller;


import java.util.HashMap;
import java.util.List;

import com.emc.paradb.advisor.plugin.Plugin;
import com.emc.paradb.advisor.ui.data_distribution.DataDistributionCB;
import com.emc.paradb.advisor.ui.summary.SummaryCB;
import com.emc.paradb.advisor.ui.transaction_distribution.TransactionDistributionCB;
import com.emc.paradb.advisor.ui.workload_distribution.WorkloadDistributionCB;



public class DisplayController
{
	private static DataDistributionCB dataDistributionCB;
	private static WorkloadDistributionCB workloadDistributionCB;
	private static TransactionDistributionCB transactionDistributionCB;
	private static SummaryCB summaryCB;
	private static int index = -1;
	
	/**
	 * register a callback interface to the controller. 
	 * the controller will invoke the interface.draw() function when the evaluating process finishes.
	 * @param dataDistributionCB
	 * @return
	 */
	public static boolean registerDataDistributionCB(DataDistributionCB dataDistributionCB)
	{
		DisplayController.dataDistributionCB = dataDistributionCB;
		return true;
	}	

	public static boolean registerWorkloadDistributionCB(WorkloadDistributionCB workloadDistributionCB)
	{
		DisplayController.workloadDistributionCB = workloadDistributionCB;
		return true;
	}	
	
	public static boolean registerTransactionDistributionCB(TransactionDistributionCB tdCB)
	{
		DisplayController.transactionDistributionCB = tdCB;
		return true;
	}
	
	public static boolean registerSummaryCB(SummaryCB sumCB)
	{
		DisplayController.summaryCB = sumCB;
		return true;
	}
	
	public static void displayCompare(HashMap<String, Float> dDVarMap, HashMap<String, Float> wDVarMap)
	{
		dataDistributionCB.draw(dDVarMap);
		workloadDistributionCB.draw(wDVarMap);
	}
	
	public static void display(int selected)
	{
		index = selected;
		if(index < 0)
			return;
		
		display();
	}
	
	public static void display()
	{
		Plugin aPlugin= AlgorithmController.getAlgorithm(index);
		
		List<Long> dataDistributionList = aPlugin.getDataDistribution();
		if(dataDistributionList != null)
			dataDistributionCB.draw(dataDistributionList);
		
		List<Long> workloadDistributionList = aPlugin.getWorkloadDistribution();
		if(workloadDistributionList != null)
			workloadDistributionCB.draw(workloadDistributionList);
		
		int dist = aPlugin.getDist();
		int nonDist = aPlugin.getNonDist();
		HashMap<Integer, Integer> nodeAccess = aPlugin.getNodeAccess();
		if(dist != -1 && nonDist != -1 && nodeAccess != null)
			transactionDistributionCB.draw(dist, nonDist, nodeAccess);
	}
	
	public static void displaySummary(Object[][] data, Object[] columnNames)
	{
		summaryCB.drawSummaryTable(data, columnNames);
	}
}