package com.emc.paradb.advisor.plugin;

import java.util.HashMap;
import java.util.List;

import com.emc.paradb.advisor.evaluator.WorkloadDistributionEva;


public class Plugin
{
	private String name = null;
	private String id = null;
	private String partitionMethod = null;
	private String placementMethod = null;
	private String description = null;
	private PlugInterface instance = null;
	private List<Long> dataDistributionList = null;
	private List<Long> workloadDistributionList = null;
	private int dist = -1;
	private int nonDist = -1;
	HashMap<Integer, Integer> nodeAccess = null;
	
	public Plugin()
	{
		
	}
	public boolean setInfo(String id, String partitionMethod, String placementMethod, String descrip)
	{
		this.id = id;
		this.partitionMethod = partitionMethod;
		this.placementMethod = placementMethod;
		this.description = descrip;
		
		int index = id.lastIndexOf(".");
		name = id.substring(index + 1);
		return true;
	}
	
	public boolean setInfo(PluginInfo info)
	{
		name = info.className;
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
	
	public void setDataDistribution(List<Long> dataDistributionList)
	{
		this.dataDistributionList = dataDistributionList;
	}
	public void setWorkloadDistribution(List<Long> workloadDistributionList)
	{
		this.workloadDistributionList = workloadDistributionList;
	}
	public void setTransactionDistribution(int dist, int nonDist, HashMap<Integer, Integer> nodeAccess)
	{
		this.dist = dist;
		this.nonDist = nonDist;
		this.nodeAccess = nodeAccess;
	}
	
	public List<Long> getDataDistribution()
	{
		return dataDistributionList;
	}
	public List<Long> getWorkloadDistribution()
	{
		return workloadDistributionList;
	}
	public String getName()
	{
		return name;
	}
	public int getDist()
	{
		return dist;
	}
	public int getNonDist()
	{
		return nonDist;
	}
	public HashMap<Integer, Integer> getNodeAccess()
	{
		return nodeAccess;
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