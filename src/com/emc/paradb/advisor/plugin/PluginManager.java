package com.emc.paradb.advisor.plugin;

import java.util.List;


public class PluginManager
{
	
	public static List<PlugInterface> pluginInterfaces = null;
	
	public PluginManager(){
		
	}
	public static List<PlugInterface> loadPlugin(){

		PluginReader pluginReader = new PluginReader();
		pluginInterfaces = pluginReader.getPluginCollection();	
		
		return pluginInterfaces;
	}
}