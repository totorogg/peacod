package com.emc.paradb.advisor.ui.workload_distribution;

import java.util.HashMap;
import java.util.List;

/**
 * the call back interface for workload distribution display panel
 * it should be defined by the workload distribution class and registered in controller
 * 
 * @author Xin Pan
 *
 */
public interface WorkloadDistributionCB
{
	public void draw(List<Long> dataSet);
	public void draw(HashMap<String, Float> wDVarMap);
}