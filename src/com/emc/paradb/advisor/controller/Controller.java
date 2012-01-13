package com.emc.paradb.advisor.controller;

import com.emc.paradb.advisor.data_loader.DBData;
import com.emc.paradb.advisor.data_loader.DataLoader;
import com.emc.paradb.advisor.data_loader.PostgreSQLLoader;
import com.emc.paradb.advisor.workload_loader.Transaction;
import com.emc.paradb.advisor.workload_loader.Workload;
import com.emc.paradb.advisor.workload_loader.WorkloadLoader;

public class Controller
{
	protected static int nodes = 0;
	protected static boolean prepared = false;
	protected static DataLoader dataLoader = null;
	protected static WorkloadLoader workloadLoader = null;
	

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