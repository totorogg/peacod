package com.emc.paradb.advisor.controller;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.emc.paradb.advisor.algorithm.AlgorithmFactory;
import com.emc.paradb.advisor.data_loader.DBData;
import com.emc.paradb.advisor.data_loader.DataLoader;
import com.emc.paradb.advisor.evaluator.Comparator;
import com.emc.paradb.advisor.evaluator.DataDistributionEva;
import com.emc.paradb.advisor.evaluator.WorkloadDistributionEva;
import com.emc.paradb.advisor.plugin.PlugInterface;
import com.emc.paradb.advisor.plugin.Plugin;
import com.emc.paradb.advisor.ui.data_distribution.DataDistributionCB;
import com.emc.paradb.advisor.ui.transaction_distribution.TransactionDistributionCB;
import com.emc.paradb.advisor.ui.workload_distribution.WorkloadDistributionCB;
import com.emc.paradb.advisor.workload_loader.Transaction;
import com.emc.paradb.advisor.workload_loader.Workload;


public class EvaluateController extends Controller
{
	
	
	public static void evaluate()
	{
		if(!prepared)
		{
			System.out.println("new to prepare the simulator first");
			return;
		}
		
		DBData dbData = DataLoader.getDBData();
		Connection conn = DataLoader.getConn();
		Workload<Transaction<Object>> workload = workloadLoader.getWorkload();
		List<Plugin> algorithms = AlgorithmFactory.getSelectedAlgorithms();
		
		int curAlgo = 0;
		float algoNum = algorithms.size();
		for (Plugin aAlgorithm : algorithms) 
		{
			progressBar.setProgress( (int)(++curAlgo/algoNum));
			progressBar.setState(aAlgorithm.getID());
			
			PlugInterface instance = aAlgorithm.getInstance();
			instance.accept(conn, workload, dbData, nodes);


			//evaluate the data distribution
			List<Long> dataDistributionList = null;

			dataDistributionList = DataDistributionEva.evaluate(aAlgorithm, nodes);
			if(dataDistributionList != null)
				aAlgorithm.setDataDistribution(dataDistributionList);
		
			
			//evaluate the workload & transaction distribution	
			WorkloadDistributionEva.evaluate(aAlgorithm, nodes);
			
			List<Long> workloadDistributionList = null;
			workloadDistributionList = WorkloadDistributionEva.getWorkloadDistribution();
			if(workloadDistributionList != null)
				aAlgorithm.setWorkloadDistribution(workloadDistributionList);
			
			int dist = WorkloadDistributionEva.getDistCount();
			int nonDist = WorkloadDistributionEva.getNonDistCount();
			HashMap<Integer, Integer> nodeAccess = WorkloadDistributionEva.getDistCountMap();
			aAlgorithm.setTransactionDistribution(dist, nonDist, nodeAccess);
		}
		DisplayController.display();

	}
	
	
	
	
	public static void compare()
	{
		if(!prepared)
		{
			System.out.println("new to evaluate the simulator first");
			return;
		}
		
		List<Plugin> algorithms = AlgorithmFactory.getSelectedAlgorithms();
		
		for(Plugin aAlgorithm : algorithms)
		{
			if(aAlgorithm.getDataDistribution() == null)
			{
				EvaluateController.evaluate();
				break;
			}
		}
		Comparator.compare(algorithms, nodes);
		DisplayController.displayCompare(Comparator.getDataDistVar(), Comparator.getWorkloadDistVar());
	}
}