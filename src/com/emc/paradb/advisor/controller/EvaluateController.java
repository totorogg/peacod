package com.emc.paradb.advisor.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.emc.paradb.advisor.algorithm.AlgorithmFactory;
import com.emc.paradb.advisor.evaluator.DataDistributionEva;
import com.emc.paradb.advisor.evaluator.WorkloadDistributionEva;
import com.emc.paradb.advisor.plugin.Plugin;
import com.emc.paradb.advisor.ui.data_distribution.DataDistributionCB;
import com.emc.paradb.advisor.ui.transaction_distribution.TransactionDistributionCB;
import com.emc.paradb.advisor.ui.workload_distribution.WorkloadDistributionCB;


public class EvaluateController extends Controller
{
	private static DataDistributionCB dataDistributionCB;
	private static WorkloadDistributionCB workloadDistributionCB;
	private static TransactionDistributionCB transactionDistributionCB;
	
	/**
	 * register a callback interface to the controller. 
	 * the controller will invoke the interface.draw() function when the evaluating process finishes.
	 * @param dataDistributionCB
	 * @return
	 */
	public static boolean RegisterDataDistributionCB(DataDistributionCB dataDistributionCB)
	{
		EvaluateController.dataDistributionCB = dataDistributionCB;
		return true;
	}	

	public static boolean RegisterWorkloadDistributionCB(WorkloadDistributionCB workloadDistributionCB)
	{
		EvaluateController.workloadDistributionCB = workloadDistributionCB;
		return true;
	}	
	
	public static boolean RegisterTransactionDistributionCB(TransactionDistributionCB tdCB)
	{
		EvaluateController.transactionDistributionCB = tdCB;
		return true;
	}
	
	public static void recommend(int nodes)
	{
		List<Plugin> selectedPlugins = AlgorithmFactory.getSelectedAlgorithms();
		for(Plugin aPlugin : selectedPlugins)
		{
			//evaluate the data distribution
			List<Long> dataDistributionSet = null;

			dataDistributionSet = DataDistributionEva.evaluate(aPlugin, nodes);
			if(dataDistributionSet != null)
				dataDistributionCB.draw(dataDistributionSet);
			
			//evaluate the workload & transaction distribution	
			WorkloadDistributionEva.evaluate(aPlugin, nodes);
			
			List<Long> workloadDistributionList = null;
			workloadDistributionList = WorkloadDistributionEva.getWorkloadDistribution();
			if(workloadDistributionList != null)
				workloadDistributionCB.draw(workloadDistributionList);
			
			int dist = WorkloadDistributionEva.getDistCount();
			int nonDist = WorkloadDistributionEva.getNonDistCount();
			HashMap<Integer, Integer> nodeAccess = WorkloadDistributionEva.getDistCountMap();
			transactionDistributionCB.draw(dist, nonDist, nodeAccess);
			
		}
	}
}