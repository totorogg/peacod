package com.emc.paradb.advisor.plugin;

import java.util.List;


/**
 * a plugin factory for creating plugins. new plugins should be created from this object
 * instead of new Plugin();
 * 
 * @author Xin Pan
 *
 */
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