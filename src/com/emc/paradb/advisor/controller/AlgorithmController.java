package com.emc.paradb.advisor.controller;

import java.util.List;

import com.emc.paradb.advisor.algorithm.AlgorithmFactory;
import com.emc.paradb.advisor.plugin.Plugin;
import com.emc.paradb.advisor.plugin.PluginManager;


/**
 * This controller is responsible for algorithm related operations, such as loading, select, etc.
 * 
 * @author Xin Pan
 *
 */
public class AlgorithmController extends Controller
{
	/**
	 * called when load button in the scheme(algorithm select) panel is clicked
	 * @return
	 */
	public static boolean loadAlgorithm()
	{
		AlgorithmFactory.removeAll();
		AlgorithmFactory.loadBuildin();
		AlgorithmFactory.addAlgorithms(PluginManager.loadPlugin());
		return true;
	}
	
	/**
	 * called when "use" check box is selected in the algorithm list table
	 * @param index
	 * @param isSelected
	 * @return
	 */
	public static boolean updateSelectedAlgorithm(int index, boolean isSelected)
	{
		Plugin aPlugin = AlgorithmFactory.getAlgorithms().get(index);
		if(isSelected)
			AlgorithmFactory.addSelected(aPlugin);
		else
			AlgorithmFactory.removeSelected(aPlugin);
		
		AlgorithmFactory.ListSelectedAlgorithms();
		return true;
	}
	
	/**
	 * get a specific algorithm from the algorithm list
	 * @param index
	 * @return
	 */
	public static Plugin getAlgorithm(int index)
	{
		List<Plugin> algorithms = AlgorithmFactory.getAlgorithms();
		return algorithms.get(index);
	}
	
	
	public static void updateSetting(int index)
	{
		Plugin aPlugin = AlgorithmFactory.getAlgorithms().get(index);
		if(aPlugin.getInstance().getSetting() != null)
		{
			List<String[]> paraList = aPlugin.getInstance().getSetting();
			DisplayController.displaySetting(paraList);
		}

	}
}