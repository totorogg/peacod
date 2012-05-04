package com.emc.paradb.advisor.workload_loader;

import java.util.HashSet;
import java.util.Set;

/**
 * this is a container for delete sql statement
 * its elements include where key object set
 * also a map object is defined for fast access to where key object by key name
 * @author Xin Pan
 *
 */

public class DeleteAnalysisInfo {
	
	//where clause, which has table.name = value
	private String table;
	private Set<WhereKey> whereKeys = new HashSet<WhereKey>();
	
	public Set<WhereKey> getWhereKeys() 
	{
		return whereKeys;
	}

	
	
	public String getTable() {
		return table;
	}
	public void setTable(String table) {
		this.table = table;
	}

}
