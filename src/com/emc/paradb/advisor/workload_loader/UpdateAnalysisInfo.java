package com.emc.paradb.advisor.workload_loader;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * this is a container for update sql statement
 * its elements include where key object set
 * also a map object is defined for fast access to where key object by key name
 * @author xpan
 *
 */
public class UpdateAnalysisInfo 
{	
	//where predicate which has table.key = value
	Set<WhereKey> whereKeys = new HashSet<WhereKey>();
	//map from key name to the its where key object
	HashMap<String, String> setKeys = new HashMap<String, String>();
	
	public Set<WhereKey> getWhereKeys() {
		return whereKeys;
	}

	String table;
	
	public String getTable() 
	{
		return table;
	}
	public HashMap<String, String> getSetKeys()
	{
		return setKeys;
	}
	
	public void setTable(String table) 
	{
		this.table = table;
	}
	public void setSetKeys(List<Object> keys, List<Object> expressions)
	{
		for(int i = 0; i < keys.size(); i++)
			setKeys.put(keys.get(i).toString(), expressions.get(i).toString());
	}

	
}
