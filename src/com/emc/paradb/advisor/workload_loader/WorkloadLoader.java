package com.emc.paradb.advisor.workload_loader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Random;
import java.util.Set;
import java.util.Vector;

import com.emc.paradb.advisor.controller.Controller;
import com.emc.paradb.advisor.controller.PrepareController;
import com.emc.paradb.advisor.data_loader.TableAttributes;
import com.emc.paradb.advisor.data_loader.TableNode;
import com.emc.paradb.advisor.workload_loader.SelectAnalyzer;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;

/**
 * load workloads into memory.
 * Workloads are translated into the format we define
 * 
 * @author Xin Pan
 * */
public class WorkloadLoader
{
	//selected benchmark
	private String selectedBM = null;
	//number of transactions used
	private int transactionNum = 0;
	//in-memory structure for storing workload
	private Workload<Transaction<Object>> workload = null;
	private float progress = 0;
	//determine whether use update statement or not
	protected static boolean updateFilter = false;
	
	
	public WorkloadLoader(String selectedBM, int transactionNumber)
	{
		this.selectedBM = selectedBM;
		this.transactionNum = transactionNumber;
	}
	
	public float getProgress()
	{
		return progress;
	}
	public Workload<Transaction<Object>> getWorkload()
	{
		return workload;
	}
	
	//routines for loading workload
	public void load()
	{
		new Thread()
		{
			public void run()
			{
				try 
				{
					String filePath = System.getProperty("user.dir") +"/"+ PrepareController.getBMPath();
					File file = new File(filePath);
					BufferedReader reader = new BufferedReader(new FileReader(file));

					
					String line = null;
					int loadedXact = 0;
					long readLength = 0;
					Transaction<Object> tran = null;
					workload = new Workload<Transaction<Object>>();
					
					while((line = reader.readLine()) != null)
					{
						readLength += line.getBytes().length;
						progress = (float)readLength/(file.length() * 1.1f);
						//transcations are seperated by - in files
						if(line.equalsIgnoreCase("-"))
						{
							loadedXact++;
							
							tran = new Transaction<Object>();
							workload.add(tran);
							continue;
						}
						
						Statement statement = QueryAnalyzer.analyze(line);
						//which kind of sql a statement belongs
						//different statements are stored in different objects slightly differ from each other
						if(statement instanceof Select )
						{
							SelectAnalyzer analyzer = new SelectAnalyzer();
							SelectAnalysisInfo sqlInfo = analyzer.analyze((Select)statement);
							tran.add(sqlInfo);
						}
						else if(statement instanceof Update && !updateFilter)
						{
							UpdateAnalyzer analyzer = new UpdateAnalyzer();
							UpdateAnalysisInfo sqlInfo = analyzer.analyze((Update)statement);
							tran.add(sqlInfo);
						}
						else if(statement instanceof Delete && !updateFilter)
						{
							DeleteAnalyzer analyzer = new DeleteAnalyzer();
							DeleteAnalysisInfo sqlInfo = analyzer.analyze((Delete)statement);
							tran.add(sqlInfo);
						}
						else if(statement instanceof Insert && !updateFilter)
						{
							InsertAnalyzer analyzer = new InsertAnalyzer();
							InsertAnalysisInfo sqlInfo = analyzer.analyze((Insert)statement);
							tran.add(sqlInfo);
						}
						
					}
					//sampling a subset of workload 
					sampling(loadedXact);
					//fix those keys without table name
					fix();
					progress = 1;

					reader.close();
				} 
				catch (Exception e) 
				{
					// TODO Auto-generated catch block
					System.out.println(e.getMessage());
					e.printStackTrace();
				}
			}
		}.start();
	}
	/**
	 * sampling transactions in files
	 * @param loadedXact
	 */
	private void sampling(int loadedXact)
	{
		Workload<Transaction<Object>> newWorkload = new Workload<Transaction<Object>>();
		Random r = new Random(1000000);

		//this sampling function is not quite right, but it works
		//changes are needed in the future
		for(int i = 0; i < workload.size(); i++)
			//if(r.nextInt() % loadedXact < transactionNum && newWorkload.size() < transactionNum)
			//[tag by xiaoyan: only use the front k transaction]
			if (newWorkload.size() < transactionNum)
				newWorkload.add(workload.get(i));
		workload = newWorkload;
	}
	
	//add table name to keys
	protected void fix()
	{
		HashMap<String, TableNode> tables = Controller.getData().getMetaData();
		
		for(Transaction<Object> aTran : workload)
		{
			for(Object statement : aTran)
			{
				if(statement instanceof SelectAnalysisInfo)
				{
					SelectAnalysisInfo select = (SelectAnalysisInfo)statement;
					for(WhereKey key : select.getWhereKeys())
					{
						for(String tableName : select.getTables())
						{
							if(tableName == null ||
									tables.get(tableName) == null || tables.get(tableName).getAttrVector() == null)
								System.err.println("!");
							for(TableAttributes attr : tables.get(tableName).getAttrVector())
								if(attr.getName().toUpperCase().equals(key.getKeyName().toUpperCase()))
									key.setTableName(tableName);
						}

					}
					
				}
				else if(statement instanceof UpdateAnalysisInfo)
				{
					UpdateAnalysisInfo update = (UpdateAnalysisInfo)statement;
					String tableName = update.getTable();
					for(WhereKey key : update.getWhereKeys())
					{
						if(tableName == null)
							System.out.println("1");
						else if(tables.get(tableName) == null)
							System.out.println("2");
						else if(tables.get(tableName).getAttrVector() == null)
							System.out.println("3");
						
						for(TableAttributes attr : tables.get(tableName).getAttrVector())
							if(attr.getName().toUpperCase().equals(key.getKeyName().toUpperCase()))
								key.setTableName(tableName);
						
						if(key.getTableName() == null)
							System.out.println("Error: unrecognized key" + key.getKeyName());
					}
				}
				else if(statement instanceof InsertAnalysisInfo)
				{
					//insert should always has the table name
				}
				else if(statement instanceof DeleteAnalysisInfo)
				{
					DeleteAnalysisInfo delete = (DeleteAnalysisInfo)statement;
					String tableName = delete.getTable();
					for(WhereKey key : delete.getWhereKeys())
					{	
						for(TableAttributes attr : tables.get(tableName).getAttrVector())
							if(attr.getName().toUpperCase().equals(key.getKeyName().toUpperCase()))
								key.setTableName(tableName);
						
						if(key.getTableName() == null)
							System.out.println("Error: unrecognized key" + key.getKeyName());
					}
				}
			}
		}
	}
}




