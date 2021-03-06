package com.emc.paradb.advisor.ui.transaction_distribution;

import java.util.HashMap;
import java.util.Map;

public interface TransactionDistributionCB
{
	public void draw(int dist, int nonDist, Map<Integer, Integer> nodeAccess);
	public void draw(HashMap<String, Integer> tdMap);
}