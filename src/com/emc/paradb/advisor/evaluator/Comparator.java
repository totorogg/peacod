package com.emc.paradb.advisor.evaluator;

import java.util.HashMap;
import java.util.List;

import com.emc.paradb.advisor.algorithm.AlgorithmFactory;
import com.emc.paradb.advisor.plugin.Plugin;


public class Comparator
{
	private static HashMap<String, Float> dDVarMap = null;
	private static HashMap<String, Float> wDVarMap = null;
	private static HashMap<String, Float> tDMap = null;
	private static int nodes;
	
	public static void compare(List<Plugin> algorithms, int nodes)
	{
		dDVarMap = new HashMap<String, Float>();
		wDVarMap = new HashMap<String, Float>();
		tDMap = new HashMap<String, Float>();
		Comparator.nodes = nodes;
		
		for(Plugin aAlgorithm : algorithms)
		{
			getDataDistComp(aAlgorithm);
			
			getWorkloadDistComp(aAlgorithm);
			
			getTranDistComp(aAlgorithm);
		}
	}
	
	private static void getDataDistComp(Plugin aAlgorithm)
	{
		Long dDSum =  0L;
		Long dDMax = 0L;
		List<Long> dataDistributionList = aAlgorithm.getDataDistribution();
		
		for(int i = 0; i < nodes; i++)
		{
			Long dDtmp = dataDistributionList.get(i);
			
			if(dDtmp > dDMax)
				dDMax = dDtmp;
			
			dDSum += dDtmp;
		}
		Long dDAvg = dDSum/nodes;
		
		Long dDDiffSquareSum = 0L;
		for(int i = 0; i < nodes; i++)
			dDDiffSquareSum += pow(dataDistributionList.get(i) - dDAvg, 2);

		float nomDDVar = (float) (Math.sqrt((double)dDDiffSquareSum/(double)nodes)) / (float)dDMax;	
		dDVarMap.put(aAlgorithm.getID(), nomDDVar);
	}
	
	private static void getWorkloadDistComp(Plugin aAlgorithm)
	{
		Long wDSum = 0L;
		Long wDMax = 0L;
		List<Long> workloadDistributionList = aAlgorithm.getWorkloadDistribution();
		
		for(int i = 0; i < nodes; i++)
		{
			Long wDtmp = workloadDistributionList.get(i);

			if(wDtmp > wDMax)
				wDMax = wDtmp;

			wDSum += wDtmp;
		}
		Long wDAvg = wDSum/nodes;
		
		Long wDDiffSquareSum = 0L;
		for(int i = 0; i < nodes; i++)
			wDDiffSquareSum += pow(workloadDistributionList.get(i) - wDAvg, 2);

		float nomWDVar = (float) (Math.sqrt((double)wDDiffSquareSum/(double)nodes)) / (float)wDMax;
		wDVarMap.put(aAlgorithm.getID(), nomWDVar);
	}
	
	private static void getTranDistComp(Plugin aAlgorithm)
	{
		int dist = aAlgorithm.getDist();
		int total = aAlgorithm.getNonDist() + dist;
		float distRate = (float)dist/(float)total;
		tDMap.put(aAlgorithm.getID(), distRate);
	}
	
	private static Long pow(Long num, int time)
	{
		Long result = 0L;
		
		if(time <= 1)
			return num;
		
		for(int i = 1; i < time; i++)
		{
			result = num * num;
		}
		return result;
	}
	
	public static HashMap<String, Float> getDataDistVar()
	{
		return dDVarMap;
	}
	public static HashMap<String, Float> getWorkloadDistVar()
	{
		return wDVarMap;
	}
	public static HashMap<String, Float> getTranDistVar()
	{
		return tDMap;
	}
}