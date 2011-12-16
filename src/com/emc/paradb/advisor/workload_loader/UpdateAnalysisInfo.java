package com.emc.paradb.advisor.workload_loader;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class UpdateAnalysisInfo {
	
	//where predicate which has table.key = value
	Set<WhereKey> whereKeys = new HashSet<WhereKey>();
	List<String> column;
	
	public Set<WhereKey> getWhereKeys() {
		return whereKeys;
	}

	String table;
	
	public String getTable() {
		return table;
	}
	public List<String> getColumn(){
		return column;
	}
	
	
	public void setTable(String table) {
		this.table = table;
	}
	public void setColumn(List<String> column){
		this.column = column;
	}

	
}
