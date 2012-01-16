package com.emc.paradb.advisor.plugin;

import java.util.List;


public class PluginManager
{
	public static List<Plugin> plugins = null;
	
	public PluginManager(){
		
	}
	public static List<Plugin> loadPlugin(){

		PluginReader pluginReader = new PluginReader();
		plugins = pluginReader.getPluginCollection();	
		
		return plugins;
	}
}