package com.emc.paradb.advisor.plugin;


public class Plugin
{
	private String id = null;
	private String partitionMethod = null;
	private String placementMethod = null;
	private String description = null;
	private PlugInterface instance = null;
	
	public Plugin()
	{
		
	}
	public boolean setInfo(String id, String partitionMethod, String placementMethod, String descrip)
	{
		this.id = id;
		this.partitionMethod = partitionMethod;
		this.placementMethod = placementMethod;
		this.description = descrip;
		return true;
	}
	
	public boolean setInfo(PluginInfo info)
	{
		id = info.path + "." + info.className;
		partitionMethod = info.partitionMethod;
		placementMethod = info.placementMethod;
		description = info.description;
		
		if(id == null || partitionMethod == null || placementMethod == null)
		{
			try 
			{
				throw new Exception("At least one id, partition method and one palcement method should be defined");
			} 
			catch (Exception e) 
			{
				System.out.println(e.getMessage());
				e.printStackTrace();
				return false;
			}
		}
		return true;
	}
	
	public boolean setInterface(PlugInterface instance)
	{
		this.instance = instance;
		if(this.instance == null)
		{
			try 
			{
				throw new Exception("The interface instance cannot be null");
			} 
			catch (Exception e) 
			{
				System.out.println(e.getMessage());
				e.printStackTrace();
				return false;
			}
		}
		return true;
	}
	
	public String getID()
	{
		return id;
	}
	public PlugInterface  getInstance()
	{
		return instance;
	}
	public String getPartitionMethod()
	{
		return partitionMethod;
	}
	public String getPlacementMethod()
	{
		return placementMethod;
	}
	public String getDescription()
	{
		return description;
	}
}