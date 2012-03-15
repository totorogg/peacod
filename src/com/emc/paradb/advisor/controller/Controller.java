package com.emc.paradb.advisor.controller;

import com.emc.paradb.advisor.data_loader.DBData;
import com.emc.paradb.advisor.data_loader.DataLoader;
import com.emc.paradb.advisor.data_loader.PostgreSQLLoader;
import com.emc.paradb.advisor.ui.mainframe.ProgressCB;
import com.emc.paradb.advisor.workload_loader.Transaction;
import com.emc.paradb.advisor.workload_loader.Workload;
import com.emc.paradb.advisor.workload_loader.WorkloadLoader;

/**
 * Father class of controllers.
 * it maintains some global information so as to control all parts of the projects
 * 
 * 
 * @author Xin Pan
 *
 */
public class Controller
{
	protected static int nodes = 0;
	protected static int transactionNum = 0;
	protected static boolean prepared = false;
	
	protected static DataLoader dataLoader = null;
	protected static WorkloadLoader workloadLoader = null;
	protected static ProgressCB progressBar = null;


	
	public static int getNodes()
	{
		return nodes;
	}
	public static DBData getData()
	{
		return DataLoader.getDBData();
	}
	public static Workload<Transaction<Object>> getWorkload()
	{
		return workloadLoader.getWorkload();
	}
}