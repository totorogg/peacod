package com.emc.paradb.advisor.controller;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;

import javax.swing.JOptionPane;
import javax.swing.JProgressBar;

import com.emc.paradb.advisor.algorithm.AlgorithmFactory;
import com.emc.paradb.advisor.data_loader.DBData;
import com.emc.paradb.advisor.data_loader.DataLoader;
import com.emc.paradb.advisor.data_loader.PostgreSQLLoader;
import com.emc.paradb.advisor.data_loader.TableNode;
import com.emc.paradb.advisor.plugin.KeyValuePair;
import com.emc.paradb.advisor.plugin.PlugInterface;
import com.emc.paradb.advisor.plugin.Plugin;
import com.emc.paradb.advisor.plugin.PluginManager;
import com.emc.paradb.advisor.workload_loader.Transaction;
import com.emc.paradb.advisor.workload_loader.Workload;
import com.emc.paradb.advisor.workload_loader.WorkloadLoader;



public class PrepareController extends Controller
{	
	/**
	 * start the partition suggest process
	 * step1: load data
	 * step2: load workload (must after data is loaded)
	 * step3: load algorithm and execute
	 * @param selectedDB
	 * @param selectedBM
	 * @param loadProgress
	 */
	public static void start(String selectedDB, String selectedBM, int nodeNumber,JProgressBar loadProgress)
	{
		nodes = nodeNumber;
		
		try {
			int progress = 0;
			if(!prepared)
			{
				// load data from the selected data source
				if(selectedDB.equalsIgnoreCase("postgresql"))
				{
					dataLoader = new PostgreSQLLoader(selectedBM);
					dataLoader.load();
				}
				else
				{
					System.out.println("unknown database selected");
					return;
				}
				loadProgress.setValue(progress);
				loadProgress.setString("data loading...");
				loadProgress.setStringPainted(true);
				
				while (progress != 100) 
				{
					dataLoader.getProgress();
					progress = (int) (dataLoader.getProgress() * 100);
					loadProgress.setValue(progress);
					Thread.sleep(50);
				}
				
			
				// load workload for the selected benchmark
				progress = 0;
				workloadLoader = new WorkloadLoader(selectedBM);
				workloadLoader.load();

				loadProgress.setString("workload loading...");
				loadProgress.setStringPainted(true);
				while (progress != 100) 
				{
					progress = (int) (workloadLoader.getProgress() * 100);
					loadProgress.setValue(progress);
					Thread.sleep(50);
				}
				prepared = true;
			}

			
			List<Plugin> algorithms = AlgorithmFactory.getSelectedAlgorithms();
			for (Plugin aAlgorithm : algorithms) 
			{
				PlugInterface instance = aAlgorithm.getInstance();
				Workload<Transaction<Object>> workload = workloadLoader.getWorkload();
				DBData dbData = DataLoader.getDBData();
				Connection conn = DataLoader.getConn();

				instance.accept(conn, workload, dbData, nodes);
				
				// for test
				HashMap<String, String> tableKeyMap = instance.getPartitionKey();

				for (String table : tableKeyMap.keySet()) 
				{
					System.out.println(table + ": " + tableKeyMap.get(table));
				}
			}
			loadProgress.setString("finished");
			loadProgress.setStringPainted(true);
		} 
		catch (Exception e) 
		{
			System.out.println("error in the main process");
			e.printStackTrace();
		}
	}
	
	

}