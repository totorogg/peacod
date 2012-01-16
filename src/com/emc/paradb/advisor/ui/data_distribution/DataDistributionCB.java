package com.emc.paradb.advisor.ui.data_distribution;

import java.util.HashMap;
import java.util.List;


public interface DataDistributionCB
{
	public void draw(List<Long> data);
	public void draw(HashMap<String, Float> dDVarMap);
}