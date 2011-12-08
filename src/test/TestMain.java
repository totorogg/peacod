package test;

import java.util.HashMap;
import java.util.List;

import plugin.PlugInterface;
import plugin.PluginManager;

public class TestMain {

	public static void main(String[] args) {
		
		PluginManager.loadPlugin();
		List<PlugInterface> pluginInterfaces = PluginManager.getInterfaces();
		
		for(PlugInterface aInterface : pluginInterfaces){
			HashMap<String, String> tableKeyMap = aInterface.defaultAlgorithm();
			
			for(String tableName : tableKeyMap.keySet()){
				System.out.println(tableName + ": " + tableKeyMap.get(tableName));
			}
		}
	}

}
