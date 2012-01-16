package com.emc.paradb.advisor.evaluator;

import java.util.HashMap;
import java.util.List;

import com.emc.paradb.advisor.algorithm.AlgorithmFactory;
import com.emc.paradb.advisor.plugin.Plugin;


public class Comparator
{
	private static HashMap<String, Float> dDVarMap = null;
	private static HashMap<String, Float> wDVarMap = null;
	
	public static void compare(List<Plugin> algorithms, int nodes)
	{
		dDVarMap = new HashMap<String, Float>();
		wDVarMap = new HashMap<String, Float>();
		
		for(Plugin aAlgorithm : algorithms)
		{
			Long dDSum =  0L;
			Long wDSum = 0L;
			Long dDMax = 0L;
			Long wDMax = 0L;
			List<Long> dataDistributionList = aAlgorithm.getDataDistribution();
			List<Long> workloadDistributionList = aAlgorithm.getWorkloadDistribution();
			
			for(int i = 0; i < nodes; i++)
			{
				Long dDtmp = dataDistributionList.get(i);
				Long wDtmp = workloadDistributionList.get(i);
				
				if(dDtmp > dDMax)
					dDMax = dDtmp;
				if(wDtmp > wDMax)
					wDMax = wDtmp;
				
				dDSum += dDtmp;
				wDSum += wDtmp;
			}
			Long dDAvg = dDSum/nodes;
			Long wDAvg = wDSum/nodes;
			
			Long dDDiffSquareSum = 0L;
			Long wDDiffSquareSum = 0L;
			for(int i = 0; i < nodes; i++)
			{
				dDDiffSquareSum += pow(dataDistributionList.get(i) - dDAvg, 2);
				wDDiffSquareSum += pow(workloadDistributionList.get(i) - wDAvg, 2);
			}
			float nomDDVar = (float) (Math.sqrt((double)dDDiffSquareSum/(double)nodes)) / (float)dDMax;
			float nomWDVar = (float) (Math.sqrt((double)wDDiffSquareSum/(double)nodes)) / (float)wDMax;
			
			dDVarMap.put(aAlgorithm.getID(), nomDDVar);
			wDVarMap.put(aAlgorithm.getID(), nomWDVar);
		}
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
}