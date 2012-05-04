package com.emc.paradb.advisor.workload_loader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * this is a container for insert sql statement
 * its elements include where key object set
 * also a map object is defined for fast access to where key object by key name
 * @author Xin Pan
 *
 */

public class InsertAnalysisInfo {
	
	//the table to be inserted
	private String table;	
	//the key and correspondent values of the the inserted tuple
	Map<String, String> obj = new HashMap<String, String>();
	
	private List<String> keys = new ArrayList<String>();
	private List<String> values = new ArrayList<String>();
	
	public InsertAnalysisInfo(){}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public Map<String, String> getKeyValueMap() {
		return obj;
	}
	
	public void addKey(String key){
		keys.add(key);
	}
	
	public void addValue(String value){
		values.add(value);
	}
	
	public void finalize(){
		for (int i = 0; i < keys.size(); i ++){
			obj.put(keys.get(i), values.get(i));
		}
	}
	

}
