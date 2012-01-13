package com.emc.paradb.advisor.workload_loader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

import com.emc.paradb.advisor.controller.Controller;
import com.emc.paradb.advisor.data_loader.TableAttributes;
import com.emc.paradb.advisor.data_loader.TableNode;
import com.emc.paradb.advisor.workload_loader.SelectAnalyzer;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;


public class WorkloadLoader
{
	private String fileName = "workload.log";
	private String selectedBM = null;
	private Workload<Transaction<Object>> workload = new Workload<Transaction<Object>>();
	private float progress = 0;
	
	public WorkloadLoader(String selectedBM)
	{
		this.selectedBM = selectedBM;
	}
	
	public float getProgress()
	{
		return progress;
	}
	public Workload<Transaction<Object>> getWorkload()
	{
		return workload;
	}
	
	
	public void load()
	{
		new Thread(){
			public void run()
			{
				try 
				{
					String filePath = System.getProperty("user.dir") + "/workload/" + fileName;
					File file = new File(filePath);
					BufferedReader reader = new BufferedReader(new FileReader(file));

					
					String line = null;
					long readLength = 0;
					Transaction<Object> tran = null;
					while((line = reader.readLine()) != null)
					{
						readLength += line.getBytes().length;
						progress = (float)readLength/(file.length() * 1.1f);
						if(line.equalsIgnoreCase("-"))
						{
							tran = new Transaction<Object>();
							workload.add(tran);
							continue;
						}
						
						Statement statement = QueryAnalyzer.analyze(line);
						
						if(statement instanceof Select)
						{
							SelectAnalyzer analyzer = new SelectAnalyzer();
							SelectAnalysisInfo sqlInfo = analyzer.analyze((Select)statement);
							tran.add(sqlInfo);
						}
						else if(statement instanceof Update)
						{
							UpdateAnalyzer analyzer = new UpdateAnalyzer();
							UpdateAnalysisInfo sqlInfo = analyzer.analyze((Update)statement);
							tran.add(sqlInfo);
						}
						else if(statement instanceof Delete)
						{
							DeleteAnalyzer analyzer = new DeleteAnalyzer();
							DeleteAnalysisInfo sqlInfo = analyzer.analyze((Delete)statement);
							tran.add(sqlInfo);
						}
						else if(statement instanceof Insert)
						{
							InsertAnalyzer analyzer = new InsertAnalyzer();
							InsertAnalysisInfo sqlInfo = analyzer.analyze((Insert)statement);
							tran.add(sqlInfo);
						}
						
					}
					//filled some entries in workload (e.g. when a=1, we should add table.a = 1).
					fix();
					progress = 1;
					/*for test
					for(Transaction<Object> aTran : workload)
					{
						for(Object statement : aTran)
						{
							if(statement instanceof SelectAnalysisInfo)
							{
								Set<WhereKey> keys = ((SelectAnalysisInfo) statement).getWhereKeys();

								for(WhereKey key : keys)
								{
									System.out.println(key.getTableName() + "." + key.getKeyName() + 
														" = " + key.getKeyValue());
								}
							}
						}
					}*/
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
	
	protected void fix()
	{
		for(Transaction<Object> aTran : workload)
		{
			for(Object statement : aTran)
			{
				if(statement instanceof SelectAnalysisInfo)
				{
					SelectAnalysisInfo select = (SelectAnalysisInfo)statement;
					
					for(WhereKey key : select.getWhereKeys())		
						updateWhereKey(key);	
					
				}
				else if(statement instanceof UpdateAnalysisInfo)
				{
					UpdateAnalysisInfo update = (UpdateAnalysisInfo)statement;
					for(WhereKey key : update.getWhereKeys())
						updateWhereKey(key);	
					
				}
				else if(statement instanceof InsertAnalysisInfo)
				{
					//insert should always has the table name
				}
				else if(statement instanceof DeleteAnalysisInfo)
				{
					DeleteAnalysisInfo delete = (DeleteAnalysisInfo)statement;
					for(WhereKey key : delete.getWhereKeys())
						updateWhereKey(key);
				}
			}
		}
	}
	
	protected void updateWhereKey(WhereKey key)
	{
		if(key.getTableName() != null)
			return;
		
		String table = null;	
		HashMap<String, TableNode> tables = Controller.getData().getMetaData();
		
		for(TableNode tableNode : tables.values())
		{
			HashMap<String, TableAttributes> attributes = tableNode.getAttributes();
			if(attributes.get(key.getKeyName()) != null)
			{
				table = tableNode.getName();
				break;
			}
		}
		
		//findTableName:
		if(table == null)
			System.out.println(String.format("%s cannot find %s", table, key));
		
		key.setTableName(table);
	}
}




