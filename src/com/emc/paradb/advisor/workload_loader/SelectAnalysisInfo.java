package com.emc.paradb.advisor.workload_loader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * this is a container for select sql statement
 * its elements include where key object set
 * also a map object is defined for fast access to where key object by key name
 * @author Xin Pan
 *
 */
public class SelectAnalysisInfo 
{
	
	//selected tables
	Set<String> tables = new HashSet<String>();
	//where predicate which includes table.key = value
	Set<WhereKey> whereKeys = new HashSet<WhereKey>();
	
	public Set<String> getTables() {
		return tables;
	}
	public Set<WhereKey> getWhereKeys() {
		return whereKeys;
	}

}
