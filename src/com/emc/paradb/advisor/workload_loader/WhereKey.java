package com.emc.paradb.advisor.workload_loader;


public class WhereKey {
	
	String tableName = null;
	String keyName = null;
	Object keyValue = null;
	
	public Object getKeyValue() {
		return keyValue;
	}
	public void setKeyValue(Object keyValue) {
		this.keyValue = keyValue;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getKeyName() {
		return keyName;
	}
	public void setKeyName(String keyName) {
		this.keyName = keyName;
	}
	
	@Override
	public int hashCode() 
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((keyName == null) ? 0 : keyName.hashCode());
		result = prime * result
				+ ((tableName == null) ? 0 : tableName.hashCode());
		return result;
	}
	public String toString()
	{
		return tableName + ":" + keyName + ":" + keyValue;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		WhereKey other = (WhereKey) obj;
		if (keyName == null) {
			if (other.keyName != null)
				return false;
		} else if (!keyName.equals(other.keyName))
			return false;
		if (tableName == null) {
			if (other.tableName != null)
				return false;
		} else if (!tableName.equals(other.tableName))
			return false;
		return true;
	}

}
