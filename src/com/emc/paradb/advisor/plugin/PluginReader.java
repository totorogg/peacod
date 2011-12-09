package com.emc.paradb.advisor.plugin;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarInputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;




class JarFilter implements FilenameFilter {

	@Override
	public boolean accept(File dir, String name) {
		// TODO Auto-generated method stub
		return (name.endsWith(".jar"));
	}
}

public class PluginReader {

	// Parameters
	private static final Class[] parameters = new Class[] { URL.class };
	private List<PlugInterface> pluginInterfaceList = null;
	

	public PluginReader(){
		
		pluginInterfaceList = new ArrayList<PlugInterface>();
		
		StringBuffer pluginDirPath = new StringBuffer(System.getProperty("user.dir")).append("\\plugin");			
		
		try{
			search(pluginDirPath.toString());
		}catch(Exception e){
			System.out.println(e.getMessage());
		}
	}

	public void search(String directory) throws Exception{
			
		File dir = new File(directory);
		if (dir.isFile()) {
			return;
		}
		
		File[] files = dir.listFiles(new JarFilter());
		for (File f : files) {
			
			JarFile jarFile = new JarFile(f.getAbsolutePath());

			ConfigXML configXML = new ConfigXML();
			List<PluginInfo> plugInfoList = configXML.parse(jarFile);

			//List<String> classNames = getClassNames(jarFile);
			for (PluginInfo aPlugin : plugInfoList) {
				
				addURL(f.toURI().toURL());
				
				String interfaceURL = new String(aPlugin.path + "." + aPlugin.className);
				interfaceURL = interfaceURL.substring(0, interfaceURL.length() - 6);
				
				Class clazz = getClass(f, interfaceURL);
				Class[] interfaces = clazz.getInterfaces();
				
				for (Class c : interfaces) {
					if (c.getName().equals("com.emc.paradb.advisor.plugin.PlugInterface")) {
						pluginInterfaceList.add((PlugInterface)clazz.newInstance());
					}
				}
			}
		}
	}

	protected List<String> getClassNames(JarInputStream jarFile) throws IOException{
		
		ArrayList<String> classes = new ArrayList<String>();
		JarEntry jarEntry;

		while (true) {
			jarEntry = jarFile.getNextJarEntry();
			if (jarEntry == null) {
				break;
			}
			if (jarEntry.getName().endsWith(".class")) {					
				classes.add(jarEntry.getName().replaceAll("/", "\\."));
			}
		}
		return classes;
	}

	public Class getClass(File jarEntry, String className) throws Exception{
		
		URLClassLoader clazzLoader;

		String entryPath = jarEntry.getAbsolutePath();
		entryPath = "jar:file://" + entryPath + "//";
	
		URL url = new File(entryPath).toURI().toURL();
		clazzLoader = new URLClassLoader(new URL[]{url});
		Class clazz = clazzLoader.loadClass(className);
			
		return clazz;
	}

	public void addURL(URL url){
		
		URLClassLoader sysLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
		URL urls[] = sysLoader.getURLs();
		for (int i = 0; i < urls.length; i++) {
			if (urls[i].toString().equalsIgnoreCase(url.toString())) {
				return;
			}
		}
	
		Class sysclass = URLClassLoader.class;
		try {
			Method method = sysclass.getDeclaredMethod("addURL", parameters);
			method.setAccessible(true);
			method.invoke(sysLoader, new Object[]{url});
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}

	
	
	public List<PlugInterface> getPluginCollection() {
		return pluginInterfaceList;
	}

	public void setPluginInterfaceList(List<PlugInterface> pluginInterfaceList) {
		this.pluginInterfaceList = pluginInterfaceList;
	}
}




class ConfigXML 
{
	private List<PluginInfo> pluginInfoList = new ArrayList<PluginInfo>();
	
	public List<PluginInfo> parse(JarFile jarFile){
		
		
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = null;
		Document doc = null;
		
		JarEntry jarEntry = jarFile.getJarEntry("config.xml");
		
		try {
			dBuilder = dbFactory.newDocumentBuilder();
			doc = dBuilder.parse(jarFile.getInputStream(jarEntry));
			
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		
		doc.getDocumentElement().normalize();	
		
		NodeList nodeList = doc.getElementsByTagName("plugin");
		for(int i = 0; i < nodeList.getLength(); i++){
			
			Element aElement = (Element)nodeList.item(i);
			
			PluginInfo aPlugin = new PluginInfo();
			aPlugin.className = getTagValue("class", aElement);
			aPlugin.path = getTagValue("path", aElement);
			aPlugin.interf = getTagValue("interface", aElement);
			
			pluginInfoList.add(aPlugin);
		}
		
		return pluginInfoList;
	}
	
	public static String getTagValue(String tagName, Element aElement){
		
		NodeList nList = aElement.getElementsByTagName(tagName).item(0).getChildNodes();
		Node nValue = (Node)nList.item(0);
		return nValue.getNodeValue();
	}

}


class PluginInfo
{
	public String path = null;
	public String className = null;
	public String interf = null;
}










