package com.emc.paradb.advisor.algorithm;

import java.util.ArrayList;
import java.util.List;

import com.emc.paradb.advisor.plugin.PlugInterface;


public class AlgorithmFactory
{
	private static List<PlugInterface> plugAlgorithms = new ArrayList<PlugInterface>();
	private static List<PlugInterface> buildInAlgorithms = new ArrayList<PlugInterface>();
	
	static
	{
		//PlugInterface navieAlgorithm = new NaiveAlgorithm();
		//buildInAlgorithms.add(navieAlgorithm);
	}
	
	public static void addPlugAlgorithm(PlugInterface aAlgorithm)
	{
		plugAlgorithms.add(aAlgorithm);
	}
	public static void removePlugAlgorithm(PlugInterface aAlgorithm)
	{
		plugAlgorithms.remove(aAlgorithm);
	}
	
	public static List<PlugInterface> getPlugAlgorithm()
	{
		return plugAlgorithms;
	}
	public static List<PlugInterface> getBuildInAlgorithm()
	{
		return buildInAlgorithms;
	}
	
	public static void ListInterfaces()
	{
		for(PlugInterface aAlgorithm : plugAlgorithms)
		{
			System.out.println(aAlgorithm);
		}
	}
}