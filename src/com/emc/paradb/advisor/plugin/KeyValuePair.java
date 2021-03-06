package com.emc.paradb.advisor.plugin;

import com.emc.paradb.advisor.workload_loader.WhereKey.Range;


/**
 * this class serve as a container for a key value pair.
 * It also contains the operation the key value pair will perform on the database
 * note that we only take "key" and "value" into account when considering hashCode and equals
 * @author Xin Pan
 *
 */
public class KeyValuePair
{
	private String table = null;
	private String key = null;
	private String value = null;
	private String opera = null;
	private long cardinality = 0;
	private Range range = Range.EQUAL;
	
	
	
	public KeyValuePair(String key, String value)
	{
		this.key = key;
		this.value = value;
	}
	public KeyValuePair(String table, String key, String value)
	{
		this.table = table;
		this.key = key;
		this.value = value;
	}
	public KeyValuePair(String key, String value, long card)
	{
		this.key = key;
		this.value = value;
		this.cardinality = card;
	}
	public String getKey()
	{
		return key;
	}
	public String getValue()
	{
		return value;
	}
	public long getCard()
	{
		return cardinality;
	}
	public String getTable()
	{
		return table;
	}
	public String getOpera()
	{
		return opera;
	}
	public Range getRange()
	{
		return range;
	}
	
	public void setOpera(String opera)
	{
		this.opera = opera;
	}
	public void setRange(Range range)
	{
		this.range = range;
	}
	
	
	public boolean equals(Object obj)
	{
		if(this == obj)
			return true;
		if(obj == null)
			return false;
		if(!(obj instanceof KeyValuePair))
			return false;
	
		KeyValuePair kvPair = (KeyValuePair)obj;
		if(kvPair.getKey().equals(key) && 
		   kvPair.getValue().equals(value))
			return true;
		
		return false;
	}
	public int hashCode()
	{
		return 7 * key.hashCode() + 13 * value.hashCode();
	}
	
	public String toString()
	{
		return "table" + table + 
				"\tkey:" + key + 
				"\tvalue:" + value;
	}
}