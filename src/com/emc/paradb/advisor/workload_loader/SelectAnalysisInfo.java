package com.emc.paradb.advisor.workload_loader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class SelectAnalysisInfo {
	
	//selected tables
	Set<String> tables = new HashSet<String>();
	//the joins info in the select statement
	List<JoinNode> joins = new ArrayList<JoinNode>();
	//where predicate which includes table.key = value
	Set<WhereKey> whereKeys = new HashSet<WhereKey>();
	
	public Set<String> getTables() {
		return tables;
	}
	public List<JoinNode> getJoins() {
		return joins;
	}
	public Set<WhereKey> getWhereKeys() {
		return whereKeys;
	}
	
}
