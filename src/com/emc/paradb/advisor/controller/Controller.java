package com.emc.paradb.advisor.controller;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;

import javax.swing.JOptionPane;
import javax.swing.JProgressBar;

import com.emc.paradb.advisor.algorithm.AlgorithmFactory;
import com.emc.paradb.advisor.data_loader.DBData;
import com.emc.paradb.advisor.data_loader.PostgreSQLLoader;
import com.emc.paradb.advisor.data_loader.TableNode;
import com.emc.paradb.advisor.plugin.KeyValuePair;
import com.emc.paradb.advisor.plugin.PlugInterface;
import com.emc.paradb.advisor.plugin.Plugin;
import com.emc.paradb.advisor.plugin.PluginManager;
import com.emc.paradb.advisor.workload_loader.Transaction;
import com.emc.paradb.advisor.workload_loader.Workload;
import com.emc.paradb.advisor.workload_loader.WorkloadLoader;



public class Controller
{
	private static boolean loadAlgorithm = false;
	private static boolean loadWorkload = false;
	private static boolean loadData = false;
	private static PostgreSQLLoader dataLoader = null;
	private static WorkloadLoader workloadLoader = null;
	
	/**
	 * start the partition suggest process
	 * step1: load workload
	 * step2: load data
	 * step3: load algorithm and execute
	 * @param selectedDB
	 * @param selectedBM
	 * @param loadProgress
	 */
	public static void start(String selectedDB, String selectedBM, JProgressBar loadProgress)
	{
		if(!loadAlgorithm)
		{
			JOptionPane.showMessageDialog(null, "Algorithms should be loaded first");
			return;
		}
		
		try {
			int progress = 0;

			// load workload for the selected benchmark
			if(loadWorkload == false)
			{
				workloadLoader = new WorkloadLoader(selectedBM);
				workloadLoader.load();
				
				loadProgress.setString("workload loading...");
				loadProgress.setStringPainted(true);
				while (progress != 100) 
				{
					progress = (int)(workloadLoader.getProgress() * 100);
					loadProgress.setValue(progress);
					Thread.sleep(50);
				}
				loadWorkload = true;
			}
			
			// load data from the selected data source
			if(loadData == false)
			{
				dataLoader = new PostgreSQLLoader(selectedBM);
				dataLoader.load();

				progress = 0;
				loadProgress.setValue(progress);
				loadProgress.setString("data loading...");
				loadProgress.setStringPainted(true);
				while (progress != 100) {
					dataLoader.getProgress();
					progress = (int) (dataLoader.getProgress() * 100);
					loadProgress.setValue(progress);
					Thread.sleep(50);
				}
				loadData = true;
			}

			
			List<Plugin> algorithms = AlgorithmFactory.getSelectedAlgorithms();
			
			for (Plugin aAlgorithm : algorithms) 
			{
				PlugInterface instance = aAlgorithm.getInstance();
				Workload<Transaction<Object>> workload = workloadLoader.getWorkload();
				DBData dbData = dataLoader.getDBData();
				Connection conn = dataLoader.getConn();

				instance.accept(conn, workload, dbData, 10);
				
				
				HashMap<String, String> tableKeyMap = instance.getPartitionKey();
				
				// /* for test
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
	
	
	
	public static boolean loadAlgorithm()
	{
		AlgorithmFactory.addAlgorithms(PluginManager.loadPlugin());
		loadAlgorithm = true;
		return true;
	}
	
	public static boolean updateSelectedAlgorithm(int index, boolean isSelected)
	{
		Plugin aPlugin = AlgorithmFactory.getAlgorithms().get(index);
		if(isSelected)
			AlgorithmFactory.addSelected(aPlugin);
		else
			AlgorithmFactory.removeSelected(aPlugin);
		
		AlgorithmFactory.ListSelectedAlgorithms();
		
		return true;
	}
	
	
	public static Plugin getAlgorithm(int index)
	{
		List<Plugin> algorithms = AlgorithmFactory.getAlgorithms();
		return algorithms.get(index);
	}
}