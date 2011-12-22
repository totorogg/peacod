package com.emc.paradb.advisor.algorithm;

import java.util.ArrayList;
import java.util.List;

import com.emc.paradb.advisor.plugin.PlugInterface;


public class AlgorithmFactory
{
	private static List<PlugInterface> algorithms = new ArrayList<PlugInterface>();
	private static List<PlugInterface> selectedAlgorithms = new ArrayList<PlugInterface>();
	
	static
	{
		PlugInterface countMaxRR = new CountMaxRR();

		algorithms.add(countMaxRR);
	}
	public static void ListAlgorithms()
	{
		for(PlugInterface aAlgorithm : algorithms)
			System.out.println(aAlgorithm);
	}
	public static List<PlugInterface> getAlgorithms()
	{
		return algorithms;
	}
	public static void addAlgorithms(List<PlugInterface> newAlgorithms)
	{
		algorithms.addAll(newAlgorithms);
	}
	
	
	public static void addSelected(PlugInterface aAlgorithm)
	{
		selectedAlgorithms.add(aAlgorithm);
	}
	public static void removeSelected(PlugInterface aAlgorithm)
	{
		selectedAlgorithms.remove(aAlgorithm);
	}
	public static void ListSelectedAlgorithms()
	{
		for(PlugInterface aAlgorithm : selectedAlgorithms)
			System.out.println(aAlgorithm);
	}
	public static List<PlugInterface> getSelectedAlgorithms()
	{
		return selectedAlgorithms;
	}
	

}