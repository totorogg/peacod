package com.emc.paradb.advisor.workload_loader;


/**
 * valueRange has four value
 * 1.euqal 2.smallerThan 3.largerThan 4.all
 * other value can be added in the future
 * such
 * @author Xin Pan
 *
 */
public class WhereKey 
{
	private String tableName = null;
	private String keyName = null;
	private String keyValue = null;
	private Range range = Range.EQUAL;
	
	public enum Range{
		EQUAL, SMALLEQL, LARGEEQL,
		SMALLER, LARGER, ALL
	};
	
	public String getKeyValue() {
		return keyValue;
	}
	public void setKeyValue(String keyValue) {
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
	public void setRange(Range type)
	{
		this.range = type;
	}
	public Range getRange()
	{
		return this.range;
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
