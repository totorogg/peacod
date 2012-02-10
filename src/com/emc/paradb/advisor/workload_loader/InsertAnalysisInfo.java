package com.emc.paradb.advisor.workload_loader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InsertAnalysisInfo {
	
	//the table to be inserted
	private String table;	
	//the key and correspondent values of the the inserted tuple
	Map<String, Object> obj = new HashMap<String, Object>();
	
	private List<String> keys = new ArrayList<String>();
	private List<Object> values = new ArrayList<Object>();
	
	public InsertAnalysisInfo(){}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public Map<String, Object> getKeyValueMap() {
		return obj;
	}
	
	public void addKey(String key){
		keys.add(key);
	}
	
	public void addValue(Object value){
		values.add(value);
	}
	
	public void finalize(){
		for (int i = 0; i < keys.size(); i ++){
			obj.put(keys.get(i), values.get(i));
		}
	}
	

}
