package plugin;

import java.util.List;

public class PluginManager {

	public static List<PlugInterface> pluginInterfaces = null;

	public PluginManager() {

	}

	public static void loadPlugin() {

		PluginReader pluginReader = new PluginReader();
		pluginInterfaces = pluginReader.getPluginCollection();
	}

	public static List<PlugInterface> getInterfaces() {
		return pluginInterfaces;
	}

}