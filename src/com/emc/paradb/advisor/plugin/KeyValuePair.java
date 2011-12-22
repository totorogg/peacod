package com.emc.paradb.advisor.plugin;


public class KeyValuePair
{
	private String key;
	private String value;
	
	public KeyValuePair(String key, String value)
	{
		this.key = key;
		this.value = value;
	}
	public String getKey()
	{
		return key;
	}
	public String getValue()
	{
		return value;
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
		if(kvPair.getKey().equals(key) && kvPair.getValue().equals(value))
			return true;
		
		return false;
	}
	public int hashCode()
	{
		return 7 * key.hashCode() + 13 * value.hashCode();
	}
}