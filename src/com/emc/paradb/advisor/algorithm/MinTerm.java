package com.emc.paradb.advisor.algorithm;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

import com.emc.paradb.advisor.data_loader.DBData;
import com.emc.paradb.advisor.data_loader.TableAttributes;
import com.emc.paradb.advisor.data_loader.TableNode;
import com.emc.paradb.advisor.plugin.KeyValuePair;
import com.emc.paradb.advisor.plugin.PlugInterface;
import com.emc.paradb.advisor.utils.QueryPrepare;
import com.emc.paradb.advisor.workload_loader.DeleteAnalysisInfo;
import com.emc.paradb.advisor.workload_loader.InsertAnalysisInfo;
import com.emc.paradb.advisor.workload_loader.SelectAnalysisInfo;
import com.emc.paradb.advisor.workload_loader.Transaction;
import com.emc.paradb.advisor.workload_loader.UpdateAnalysisInfo;
import com.emc.paradb.advisor.workload_loader.WhereKey;
import com.emc.paradb.advisor.workload_loader.WhereKey.Range;
import com.emc.paradb.advisor.workload_loader.Workload;

public class MinTerm implements PlugInterface
{
	Connection conn = null;
	Workload<Transaction<Object>> workload = null;
	DBData dbData = null;
	int nodes = 0;
	private int nullCount = 0;
	
	HashMap<String, TablePartition> tablePartitions = null;
	HashMap<String, List<String>> tableKeyMap = null;
	
	@Override
	public boolean accept(Connection conn, Workload<Transaction<Object>> workload,
			DBData dbData, int nodes) {

		this.conn = conn;
		this.workload = workload;
		this.dbData = dbData;
		this.nodes = nodes;
		tableKeyMap = new HashMap<String, List<String>>();
		tablePartitions = new HashMap<String, TablePartition>();
		try
		{
			setPartition();
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		return true;
	}

	private void setPartition()
	{
		preparePredicates();
		
		for(Transaction<Object> aTran : workload)
		{
			extractTran(aTran);
		}
		//System.out.println(nullCount);
		
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter("minterm"));
			listPartitions(out);
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	private void listPartitions(BufferedWriter out) throws IOException
	{
		for(TablePartition aTablePartition : tablePartitions.values())
		{
			out.write(aTablePartition.getName() + "\t");
			aTablePartition.listPartitions(out);
		}
	}
	
	private void extractTran(Transaction<Object> aTran)
	{
		for(Object statement : aTran)
		{
			if(statement instanceof SelectAnalysisInfo)
			{
				SelectAnalysisInfo select = (SelectAnalysisInfo)statement;
				
				for(WhereKey key : select.getWhereKeys())
				{						
					String tableName = key.getTableName();
					TablePartition aTablePartition = tablePartitions.get(tableName);
					
					String aKey = key.getKeyName();
					KeyPartition aKeyPartition = aTablePartition.getKeyPartition(aKey);
					if(aKeyPartition == null)
						continue;
					
					Predicate aPredicate = extractPredicate(aKey, key.getKeyValue(), key.getRange());
					if(aPredicate != null)
						aKeyPartition.addPredicate(aPredicate);
				}
			}
			else if(statement instanceof UpdateAnalysisInfo)
			{
				UpdateAnalysisInfo update = (UpdateAnalysisInfo)statement;
				for(WhereKey key : update.getWhereKeys())
				{
					String tableName = key.getTableName();
					TablePartition aTablePartition = tablePartitions.get(tableName);
					
					String aKey = key.getKeyName();
					KeyPartition aKeyPartition = aTablePartition.getKeyPartition(aKey);
					if(aKeyPartition == null)
						continue;
					
					Predicate aPredicate = extractPredicate(aKey, key.getKeyValue(), key.getRange());
					if(aPredicate != null)
						aKeyPartition.addPredicate(aPredicate);
				}
			}
			else if(statement instanceof InsertAnalysisInfo)
			{
				InsertAnalysisInfo insert = (InsertAnalysisInfo)statement;
				
				for(String key : insert.getKeyValueMap().keySet())
				{
					String tableName = insert.getTable();
					TablePartition aTablePartition = tablePartitions.get(tableName);
					
					KeyPartition aKeyPartition = aTablePartition.getKeyPartition(key);
					if(aKeyPartition == null)
						continue;
					
					Predicate aPredicate = extractPredicate(key, insert.getKeyValueMap().get(key), Range.EQUAL);
					if(aPredicate != null)
						aKeyPartition.addPredicate(aPredicate);
				}
			}
			else if(statement instanceof DeleteAnalysisInfo)
			{
				DeleteAnalysisInfo delete = (DeleteAnalysisInfo)statement;
				for(WhereKey key : delete.getWhereKeys())
				{
					String tableName = key.getTableName();
					TablePartition aTablePartition = tablePartitions.get(tableName);
					
					String aKey = key.getKeyName();
					KeyPartition aKeyPartition = aTablePartition.getKeyPartition(aKey);
					if(aKeyPartition == null)
						continue;
					
					Predicate aPredicate = extractPredicate(aKey, key.getKeyValue(), key.getRange());
					if(aPredicate != null)
						aKeyPartition.addPredicate(aPredicate);
				}
			}
		}
	}
	
	public Predicate extractPredicate(String keyName, String keyValue, Range range)
	{
		if(keyValue == null)
		{
			nullCount++;
			return null;
		}
		Predicate newPredicate = new Predicate();
	
		int value = 0;
		try
		{
			value = Integer.valueOf(keyValue);
		}catch(NumberFormatException e)
		{
			//e.printStackTrace();
			//System.err.println("Not a Integer: " + keyValue);
			return null;
		}
		
		if(range == Range.EQUAL)
		{
			newPredicate.setMax(value + 1);
			newPredicate.setMin(value);
		}
		else if(range == Range.LARGEEQL)
		{
			newPredicate.setMin(value);
		}
		else if(range == Range.LARGER)
		{
			newPredicate.setMin(value + 1);
		}
		else if(range == Range.SMALLEQL)
		{
			newPredicate.setMax(value + 1);
		}
		else if(range == Range.SMALLER)
		{
			newPredicate.setMax(value);
		}
		else
		{
			try {
				throw new Exception("Unsupported Range");
				} catch (Exception e) {
				// TODO Auto-generated catch block
				System.err.println(e.getMessage());
				e.printStackTrace();
				return null;
			}
		}
		
		return newPredicate;
	}
	
	public void preparePredicates()
	{
		HashMap<String, TableNode> tables = dbData.getMetaData();
		for(TableNode aTableNode : tables.values())
		{
			TablePartition aTablePartition = new TablePartition(aTableNode, conn);
			tablePartitions.put(aTableNode.getName(), aTablePartition);
		}
	}
	
	@Override
	public HashMap<String, List<String>> getPartitionKey() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Integer> getNode(List<KeyValuePair> kvPairs) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String[]> getSetting() {
		// TODO Auto-generated method stub
		return null;
	}
	
	
}

class TablePartition
{
	private String tableName;
	HashMap<String, KeyPartition> keyPartitions = new HashMap<String, KeyPartition>();
	
	public TablePartition(String tableName)
	{
		this.tableName = tableName;
	}
	
	public TablePartition(TableNode aTableNode, Connection conn)
	{
		tableName = aTableNode.getName();
		
		Vector<TableAttributes> attrs = aTableNode.getAttrVector();

		for(TableAttributes aAttr : attrs)
		{
			String aKey = aAttr.getName();
			try
			{
				Statement stmt = conn.createStatement();
				ResultSet minResult = stmt.executeQuery("select min(" + aKey +") " +
														"from " + QueryPrepare.prepare(tableName) + ";");
				
				int type = minResult.getMetaData().getColumnType(1);
				if(type != Types.INTEGER)
					continue;
				
				minResult.next();
				int min = minResult.getInt(1);
			
				ResultSet maxResult = stmt.executeQuery("select max(" + aKey +") " +
														"from " + QueryPrepare.prepare(tableName) + ";");
				maxResult.next();
				int max = maxResult.getInt(1);
		
				KeyPartition aKeyPartition = new KeyPartition(aKey, min, max);
				keyPartitions.put(aKey, aKeyPartition);
			}
			catch(SQLException e)
			{
				System.err.println(e.getMessage());
				e.printStackTrace();
			}
		}
	}
	
	public void listPartitions(BufferedWriter out) throws IOException
	{
		for(KeyPartition aKeyPartition : keyPartitions.values())
		{
			out.write(aKeyPartition.getName() + "\n");
			aKeyPartition.listPartitions(out);
		}
		System.out.println("");
	}
	
	public String getName()
	{
		return tableName;
	}
	
	public KeyPartition getKeyPartition(String key)
	{
		return keyPartitions.get(key);
	}
	
}

class KeyPartition
{
	private String key;
	private List<Predicate> predicates = new ArrayList<Predicate>();
	private Integer min = null;
	private Integer max = null;
	
	public KeyPartition(String key, Integer min, Integer max)
	{
		this.key = key;
		this.max = max;
		this.min = min;
		
		Predicate aPredicate = new Predicate(min, max);
		predicates.add(aPredicate);
	}
	
	public String getName()
	{
		return key;
	}
	
	public boolean addPredicate(Predicate aPredicate)
	{
		if(aPredicate.getMin() == null)
		{
			int aMax = aPredicate.getMax();
			for(int i = 0; i < predicates.size(); i++)
			{
				if(predicates.get(i).getMax() == aMax)
					return false;
				else if(predicates.get(i).getMax() > aMax)
				{
					Predicate oldPredicate = predicates.get(i);
					
					aPredicate.setMin(oldPredicate.getMin());
					oldPredicate.setMin(aMax);
					predicates.add(i, aPredicate);
					return true;
				}
			}
		}
		else if(aPredicate.getMax() == null)
		{
			int aMin = aPredicate.getMin();
			for(int i = 0; i < predicates.size(); i++)
			{
				if(predicates.get(i).getMin() == aMin)
					return false;
				else if(predicates.get(i).getMin() < aMin)
				{
					Predicate oldPredicate = predicates.get(i);
					
					aPredicate.setMin(oldPredicate.getMax());
					oldPredicate.setMax(aMin);
					predicates.add(i+1, aPredicate);
					return true;
				}
			}
		}
		else
		{
			int aMin = aPredicate.getMin();
			int aMax = aPredicate.getMax();
			
			for (int i = 0; i < predicates.size(); i++) 
			{
				Predicate oldPredicate = predicates.get(i);
				if (aMin + 1 <= oldPredicate.getMax() && 
						aMin - 1 >= oldPredicate.getMin()) 
				{
					if (aMax < oldPredicate.getMax()) 
					{
						Predicate first = new Predicate(aMin, aMax);
						Predicate second = new Predicate(aMax, oldPredicate.getMax());
						oldPredicate.setMax(aMin);
						
						predicates.add(i + 1, second);
						predicates.add(i + 1, first);
						return true;// it is important, avoid counting aMax again
					} 
					else 
					{
						Predicate first = new Predicate(aMin, oldPredicate.getMax());
						oldPredicate.setMax(aMin);
						predicates.add(i + 1, first);
					}
				}
				if(aMax +1 <= oldPredicate.getMax() &&
						aMax - 1 >= oldPredicate.getMin())
				{
					Predicate first = new Predicate(oldPredicate.getMin(), aMax);
					oldPredicate.setMin(aMax);
					predicates.add(i, first);
					return true;
				}
			}
		}
		return false;
	}
	
	public void listPartitions(BufferedWriter out) throws IOException
	{
		for(int i = 0; i < predicates.size(); i++)
		{
			Predicate aPredicate = predicates.get(i);
			out.write(aPredicate.getMin() + ":" + aPredicate.getMax() + "\t");
		}
		out.write("\t");
	}
}

class Predicate
{
	private Integer min = null;
	private Integer max = null;
	
	public Predicate()
	{	}
	
	public Predicate(int min, int max)
	{
		this.min = min;
		this.max = max;
	}
	
	public Integer getMin()
	{
		return min;
	}
	public Integer getMax()
	{
		return max;
	}
	
	public void setMin(int min)
	{
		this.min = min;
	}
	public void setMax(int max)
	{
		this.max = max;
	}
}