package com.emc.paradb.advisor.ui.data_distribution;

import java.util.HashMap;
import java.util.List;

/**
 * data distribution call back.
 * draw data distribution content
 * 
 * the interface needs to be defined by data distribution panel
 * and registered in controller.
 * 
 * controll will call draw() method after finishing evaluation
 * @author xpan
 *
 */
public interface DataDistributionCB
{
	public void draw(List<Long> data);
	public void draw(HashMap<String, Float> dDVarMap);
}