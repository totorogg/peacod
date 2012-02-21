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
	private List<Plugin> plugins = null;
	

	public PluginReader(){
		
		plugins = new ArrayList<Plugin>();
		
		StringBuffer pluginDirPath = new StringBuffer(System.getProperty("user.dir")).append("/plugin");			
		try
		{
			search(pluginDirPath.toString());
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
	}

	public void search(String directory) throws Exception{
			
		File dir = new File(directory);
		if (dir.isFile())
			return;
		
		File[] files = dir.listFiles(new JarFilter());
		for (File f : files) 
		{	
			
			JarFile jarFile = new JarFile(f.getAbsolutePath());
			ConfigXML configXML = new ConfigXML();
			PluginInfo plugInfo = configXML.parse(jarFile);
			
			Plugin aPlugin = new Plugin();
			aPlugin.setInfo(plugInfo);
			
			addURL(f.toURI().toURL());
			
			String interfaceURL = new String(plugInfo.path + "." + plugInfo.className);
			Class<?> clazz = getClass(f, interfaceURL);
			Class[] interfaces = clazz.getInterfaces();
				
			for (Class<?> c : interfaces) 
			{
				if (c.getName().equals("com.emc.paradb.advisor.plugin.PlugInterface"))
				{
					aPlugin.setInterface((PlugInterface)clazz.newInstance());
					plugins.add(aPlugin);
				}
			}
		}
	}

	protected List<String> getClassNames(JarInputStream jarFile) throws IOException
	{	
		ArrayList<String> classes = new ArrayList<String>();
		JarEntry jarEntry;

		jarEntry = jarFile.getNextJarEntry();
		while (jarEntry !=null) 
		{
			if (jarEntry.getName().endsWith(".class"))					
				classes.add(jarEntry.getName().replaceAll("/", "\\."));
				
			jarEntry = jarFile.getNextJarEntry();
		}
		return classes;
	}

	public Class<?> getClass(File jarEntry, String className) throws Exception
	{	
		URLClassLoader clazzLoader;

		String entryPath = jarEntry.getAbsolutePath();
		entryPath = "jar:file://" + entryPath + "//";
		
		URL url = new File(entryPath).toURI().toURL();
		
		clazzLoader = new URLClassLoader(new URL[]{url});
		Class<?> clazz = clazzLoader.loadClass(className);
			
		return clazz;
	}

	public void addURL(URL url)
	{	
		URLClassLoader sysLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
		URL urls[] = sysLoader.getURLs();
		for (int i = 0; i < urls.length; i++) 
		{
			if (urls[i].toString().equalsIgnoreCase(url.toString()))
				return;
		}
	
		Class<URLClassLoader> sysclass = URLClassLoader.class;
		try 
		{
			Method method = sysclass.getDeclaredMethod("addURL", parameters);
			method.setAccessible(true);
			method.invoke(sysLoader, new Object[]{url});
		} 
		catch (Throwable t)
		{
			t.printStackTrace();
		}
	}


	public List<Plugin> getPluginCollection() {
		return plugins;
	}
}




class ConfigXML 
{
	private PluginInfo pluginInfo = new PluginInfo();
	
	public PluginInfo parse(JarFile jarFile)
	{	
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = null;
		Document doc = null;
		
		JarEntry jarEntry = jarFile.getJarEntry("config.xml");
		
		try 
		{
			dBuilder = dbFactory.newDocumentBuilder();
			doc = dBuilder.parse(jarFile.getInputStream(jarEntry));	
		} 
		catch (Exception e) 
		{
			System.out.println(e.getMessage());
		}
		
		doc.getDocumentElement().normalize();	
		
		NodeList nodeList = doc.getElementsByTagName("plugin");
		Element aElement = (Element)nodeList.item(0);
			
		pluginInfo.className = getTagValue("class", aElement);
		pluginInfo.path = getTagValue("path", aElement);
		pluginInfo.interf = getTagValue("interface", aElement);
		pluginInfo.partitionMethod = getTagValue("partitionMethod", aElement);
		pluginInfo.placementMethod = getTagValue("placementMethod", aElement);
		pluginInfo.description = getTagValue("description", aElement);
		return pluginInfo;
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
	public String interf = null;
	public String className = null;
	public String partitionMethod = null;
	public String placementMethod = null;
	public String description = null;
}


