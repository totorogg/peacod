package com.emc.paradb.advisor.ui.workload_distribution;

import java.util.HashMap;
import java.util.List;


public interface WorkloadDistributionCB
{
	public void draw(List<Long> dataSet);
	public void draw(HashMap<String, Float> wDVarMap);
}