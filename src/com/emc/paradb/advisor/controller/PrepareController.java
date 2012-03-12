package com.emc.paradb.advisor.controller;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;

import javax.swing.JOptionPane;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.emc.paradb.advisor.algorithm.AlgorithmFactory;
import com.emc.paradb.advisor.data_loader.DBData;
import com.emc.paradb.advisor.data_loader.DataLoader;
import com.emc.paradb.advisor.data_loader.PostgreSQLLoader;
import com.emc.paradb.advisor.data_loader.TableNode;
import com.emc.paradb.advisor.plugin.KeyValuePair;
import com.emc.paradb.advisor.plugin.PlugInterface;
import com.emc.paradb.advisor.plugin.Plugin;
import com.emc.paradb.advisor.plugin.PluginManager;
import com.emc.paradb.advisor.ui.mainframe.ProgressCB;
import com.emc.paradb.advisor.workload_loader.Transaction;
import com.emc.paradb.advisor.workload_loader.Workload;
import com.emc.paradb.advisor.workload_loader.WorkloadLoader;



public class PrepareController extends Controller
{	
	protected static String dbIP = null;
	protected static String dbPort = null;
	protected static String dbUser = null;
	protected static String dbPassword = null;
	protected static String tatpPath = null;
	protected static String tatpDB = null;
	protected static String tpccPath = null;
	protected static String tpccDB = null;
	protected static String epinionsPath = null;
	protected static String epinionsDB = null;
	protected static String selectedBM = null;
	
	/**
	 * start the partition suggest process
	 * step1: load data
	 * step2: load workload (must after data is loaded)
	 * @param selectedDB
	 * @param selectedBM
	 * @param loadProgress
	 */
	public static void start(String selectedDB, String selectedBM, int nodeNumber, ProgressCB progressCB)
	{
		nodes = nodeNumber;
		progressBar = progressCB;
		PrepareController.selectedBM = selectedBM;
		
		try {
			int progress = 0;
			if(!prepared)
			{

				//load parameters
				if(!loadParameters())
					return;
			
				// load data from the selected data source
				if(selectedDB.equalsIgnoreCase("postgresql"))
				{
					dataLoader = new PostgreSQLLoader(selectedBM);
					dataLoader.load();
				}
				else
				{
					System.out.println("unknown database selected");
					return;
				}
				
				progressBar.setProgress(progress);
				progressBar.setState("data loading...");
				
				while (progress != 100) 
				{
					dataLoader.getProgress();
					progress = (int) (dataLoader.getProgress() * 100);
					progressBar.setProgress(progress);
					Thread.sleep(50);
				}
				
			
				// load workload for the selected benchmark
				progress = 0;
				workloadLoader = new WorkloadLoader(selectedBM);
				workloadLoader.load();

				progressBar.setState("workload loading...");
				while (progress != 100) 
				{
					progress = (int) (workloadLoader.getProgress() * 100);
					progressBar.setProgress(progress);
					Thread.sleep(50);
				}
				prepared = true;
			}

			progressBar.setState("finished");
		} 
		catch (Exception e) 
		{
			System.out.println("error in the main process");
			e.printStackTrace();
		}
	}
	
	
	private static boolean loadParameters()
	{
		String basedir = System.getProperty("user.dir") + "/";
		try{
			DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance();
			domFactory.setNamespaceAware(true);
			DocumentBuilder builder = domFactory.newDocumentBuilder();

			Document doc = builder.parse(basedir + "config.xml");
			XPath xpath = XPathFactory.newInstance().newXPath();

			XPathExpression expr = xpath
					.compile("/config/property/name[text() = \"database.ip\"]/following-sibling::*[1]");
			Object result = expr.evaluate(doc, XPathConstants.NODESET);
			dbIP = ((NodeList) result).item(0).getTextContent();
			
			expr = xpath
					.compile("/config/property/name[text() = \"database.port\"]/following-sibling::*[1]");
			result = expr.evaluate(doc, XPathConstants.NODESET);
			dbPort = ((NodeList) result).item(0).getTextContent();
			
			expr = xpath
					.compile("/config/property/name[text() = \"database.user\"]/following-sibling::*[1]");
			result = expr.evaluate(doc, XPathConstants.NODESET);
			dbUser = ((NodeList) result).item(0).getTextContent();		
			
			expr = xpath
					.compile("/config/property/name[text() = \"database.password\"]/following-sibling::*[1]");
			result = expr.evaluate(doc, XPathConstants.NODESET);
			dbPassword = ((NodeList) result).item(0).getTextContent();
			
			expr = xpath
					.compile("/config/property/name[text() = \"database.user\"]/following-sibling::*[1]");
			result = expr.evaluate(doc, XPathConstants.NODESET);
			dbUser = ((NodeList) result).item(0).getTextContent();
			
			expr = xpath
					.compile("/config/property/name[text() = \"workload.tpcc\"]/following-sibling::*[1]");
			result = expr.evaluate(doc, XPathConstants.NODESET);
			tpccPath = ((NodeList) result).item(0).getTextContent();
			expr = xpath
					.compile("/config/property/name[text() = \"workload.tpcc\"]/following-sibling::*[2]");
			result = expr.evaluate(doc, XPathConstants.NODESET);
			tpccDB = ((NodeList) result).item(0).getTextContent();
			
			expr = xpath
					.compile("/config/property/name[text() = \"workload.tatp\"]/following-sibling::*[1]");
			result = expr.evaluate(doc, XPathConstants.NODESET);
			tatpPath = ((NodeList) result).item(0).getTextContent();
			expr = xpath
					.compile("/config/property/name[text() = \"workload.tatp\"]/following-sibling::*[2]");
			result = expr.evaluate(doc, XPathConstants.NODESET);
			tatpDB = ((NodeList) result).item(0).getTextContent();
			
			expr = xpath
					.compile("/config/property/name[text() = \"workload.epinions\"]/following-sibling::*[1]");
			result = expr.evaluate(doc, XPathConstants.NODESET);
			epinionsPath = ((NodeList) result).item(0).getTextContent();
			expr = xpath
					.compile("/config/property/name[text() = \"workload.epinions\"]/following-sibling::*[2]");
			result = expr.evaluate(doc, XPathConstants.NODESET);
			epinionsDB = ((NodeList) result).item(0).getTextContent();

		}catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
		return true;
	}
	
	
	public static String getDBIP()
	{
		return dbIP;
	}
	public static String getDBPort()
	{
		return dbPort;
	}
	public static String getDBUser()
	{
		return dbUser;
	}
	public static String getDBPassword()
	{
		return dbPassword;
	}
	public static String getBMPath()
	{
		if(selectedBM.equalsIgnoreCase("tpc-c"))
			return tpccPath;
		else if(selectedBM.equalsIgnoreCase("tatp"))
			return tatpPath;
		else if(selectedBM.equalsIgnoreCase("epinions"))
			return epinionsPath;
		else
		{
			System.out.println("No support for benchMark: " + selectedBM);
			return "";
		}
	}
	public static String getDBName()
	{
		if(selectedBM.equalsIgnoreCase("tpc-c"))
			return tpccDB;
		else if(selectedBM.equalsIgnoreCase("tatp"))
			return tatpDB;
		else if(selectedBM.equalsIgnoreCase("epinions"))
			return epinionsDB;
		else
		{
			System.out.println("No support for benchMark: " + selectedBM);
			return "";
		}
	}
}