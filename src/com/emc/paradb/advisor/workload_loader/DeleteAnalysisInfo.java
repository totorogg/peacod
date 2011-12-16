package com.emc.paradb.advisor.workload_loader;

import java.util.HashSet;
import java.util.Set;

public class DeleteAnalysisInfo {
	
	//where clause, which has table.name = value
	Set<WhereKey> whereKeys = new HashSet<WhereKey>();
	
	public Set<WhereKey> getWhereKeys() {
		return whereKeys;
	}

	String table;
	
	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}	
	
}
