package com.emc.paradb.advisor.data_loader;


public class AttributeValue
{
	Object value;
	int count;
	
	public AttributeValue(Object value, int count)
	{
		this.value = value;
		this.count = count;
	}
	
	public void setValueCount(Object value, int count)
	{
		this.value = value;
		this.count = count;
	}
	public int getCount()
	{
		return count;
	}
	public Object getValue()
	{
		return value;
	}
	
	public boolean equals(Object obj)
	{
		if(this == obj)
			return true;
		if(obj == null)
			return false;
		if(!(obj instanceof AttributeValue))
			return false;
		AttributeValue aValue = (AttributeValue)obj;
		if(aValue.value.equals(value) && aValue.count == count)
			return true;
		
		return false;
	}
	
	public int hashCode()
	{
		return value.hashCode() + 7 * count;
	}
}


