package com.emc.paradb.advisor.algorithm.finegrainedgraphparititioning;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.PriorityQueue;

public class GraphFile
{
	public static File getGraphFile()
	{
		String dirName = GV.dirName;
		File dir = new File(dirName);
		File[] files = dir.listFiles(new FilenameFilter()
		{
			@Override
			public boolean accept(File arg0, String arg1) {
				// TODO Auto-generated method stub
				if(arg1.matches("[0-9]+"))
					return true;
				else 
					return false;
			}
		});
		if(files.length != 1)
			return null;
		else
			return files[0];
	}
	
	public static void sortFile() throws IOException
	{
		long start = System.currentTimeMillis();
		String dirName = GV.dirName;
		File dir = new File(dirName);
		File[] files = dir.listFiles(new FilenameFilter()
		{
			@Override
			public boolean accept(File arg0, String arg1) 
			{
				// TODO Auto-generated method stub
				//if(arg1.length() > 10)
				//{
				//	arg0.delete();
				//	return false;
				//}
				
				if(arg1.matches("[0-9]+"))
					return true;
				else 
					return false;
			}
		});
		
		 
		TreeMap<Integer, String> sortMap = new TreeMap<Integer, String>(new Comparator<Integer>()
		{
			@Override
			public int compare(Integer o1, Integer o2) 
			{
				// TODO Auto-generated method stub
				int k1 = o1.intValue();
				int k2 = o2.intValue();
				return k1 > k2 ? 1 : (k1 == k2 ? 0 : -1);
			}
			
		});
		
		for(int i = 0; i < files.length; i++)
		{
			BufferedReader in = new BufferedReader(new FileReader(files[i]));
			String line = null;
			while((line = in.readLine()) != null)
			{
				Integer aKey = Integer.valueOf(line.split("\t")[0]);
				sortMap.put(aKey, line);
			}
			in.close();
			
			boolean del = files[i].delete();
			if(del == false)
				System.err.println("cannot delete");

			
			BufferedWriter out = new BufferedWriter(new FileWriter(files[i].getAbsolutePath()));
			Iterator<Entry<Integer, String>> iterator = sortMap.entrySet().iterator();
			while(iterator.hasNext())
			{
				Entry<Integer, String> aEntry = iterator.next();
				out.write(aEntry.getValue() + "\n");
			}
			out.close();
			
			sortMap.clear();
			
			GraphFile.validate(files[i]);
		}
		long end = System.currentTimeMillis();
		System.out.println("Sort File: " + (end - start) + "ms");
	}
	
	
	
	public static boolean multiWayMerge(int nWay) throws IOException
	{
		long begin = System.currentTimeMillis();
		String dirName = GV.dirName;
		File dirFile = new File(dirName);
		File[] files = dirFile.listFiles(new FilenameFilter()
		{

			@Override
			public boolean accept(File arg0, String arg1) 
			{
				// TODO Auto-generated method stub
				if(arg1.matches("[0-9]+"))
					return true;
				return false;
			}
		});
		
		List<File> fileList = new ArrayList<File>();
		for(int i = 0; i < files.length; i++)
			fileList.add(files[i]);
		
		
		while(fileList.size() > nWay)
		{
			int start = 0;
			List<File> mergedList = new ArrayList<File>();
			for(; start + nWay <= fileList.size(); start += nWay)
			{
				File mergedFile = merge(fileList.subList(start, start + nWay));
				mergedList.add(mergedFile);
			}
			if(start < fileList.size())
			{
				File mergedFile = merge(fileList.subList(start, fileList.size()));
				mergedList.add(mergedFile);
			}
			fileList = mergedList;
		}
		merge(fileList);
		long end = System.currentTimeMillis();
		System.out.println("Merge File: " + (end - begin) + "ms");
		return true;
	}
	
	private static File merge(List<File> fileList) throws IOException
	{
		String fileName = GV.dirName +"/"+  new Date().getTime();
		
		File mergedFile = new File(fileName);
		BufferedWriter out = new BufferedWriter(new FileWriter(mergedFile));
		
		//List<Entry<BufferedReader, Integer>> sortedList = new ArrayList<Entry<BufferedReader, Integer>>();
		Comparator<Entry<BufferedReader, Integer>> cmp = new Comparator<Entry<BufferedReader, Integer>>()
		{
			@Override
			public int compare(Entry<BufferedReader, Integer> o1,
			Entry<BufferedReader, Integer> o2) 
			{
				int v1 = o1.getValue();
				int v2 = o2.getValue();
		
				return v1 > v2 ? 1 : (v1 == v2 ? 0 : -1);
			}
		};
		PriorityQueue<Entry<BufferedReader, Integer>> sortedEntries = 
				new PriorityQueue<Entry<BufferedReader, Integer>>(10, cmp);
		
		
		HashMap<BufferedReader, Integer> fileHeadMap = new HashMap<BufferedReader, Integer>();
		HashMap<BufferedReader, String> fileLineMap = new HashMap<BufferedReader, String>();
		
		for(int i = 0; i < fileList.size(); i++)
		{
			BufferedReader in = new BufferedReader(new FileReader(fileList.get(i)));
			String line = in.readLine();
			if(line != null)
			{
				int val = Integer.valueOf(line.split("\t")[0]);	
				fileHeadMap.put(in, val);
				fileLineMap.put(in, line);
			}
			else
				in.close();
		}
		sortedEntries.addAll(fileHeadMap.entrySet());
		
		Entry<BufferedReader, Integer> min = sortedEntries.poll();
		Entry<BufferedReader, Integer> nextMin = null;
		
		while(sortedEntries.size() != 0)
		{
			String minLine = fileLineMap.get(min.getKey());
			if(minLine == null)
				System.err.println("!");
			
			HashMap<Integer, Integer> adjCnt = new HashMap<Integer, Integer>();
			String[] adjs = minLine.split("\t");
			for(int i = 1; i < adjs.length; i += 2)
			{
				int adj = Integer.valueOf(adjs[i]);
				int cnt = Integer.valueOf(adjs[i+1]);
				adjCnt.put(adj, cnt);
			}
			
			updateMap(sortedEntries, fileLineMap, min);
			if(sortedEntries.size() == 0)
			{
				writeLine(min.getValue(), adjCnt, out);
				break;
			}
			
			while((nextMin = sortedEntries.poll()).getValue().equals(min.getValue()))
			{
				minLine = fileLineMap.get(nextMin.getKey());
				adjs = minLine.split("\t");
				for(int i = 1; i < adjs.length; i += 2)
				{
					int adj = Integer.valueOf(adjs[i]);
					int cnt = Integer.valueOf(adjs[i+1]);
					if(adjCnt.get(adj) != null)
						adjCnt.put(adj, adjCnt.get(adj)+cnt);
					else
						adjCnt.put(adj, cnt);
				}
				updateMap(sortedEntries, fileLineMap, nextMin);
				if(sortedEntries.size() == 0)
					break;
			}
			writeLine(min.getValue(), adjCnt, out);
					
			if(sortedEntries.size() == 0 && !nextMin.getValue().equals(min.getValue()))
			{
				BufferedReader in = nextMin.getKey();
				minLine = fileLineMap.get(in);
				if(minLine == null)
					System.err.println("!");
				
				out.write(minLine + "\n");
				
				while((minLine = in.readLine()) != null)
					out.write(minLine + "\n");
			}
			
			min = nextMin;
		}
		out.close();
		
		for(int i = 0; i < fileList.size(); i++)
			fileList.get(i).delete();
		
		validate(mergedFile);
		
		return mergedFile;
	}
	
	private static void writeLine(int pos, HashMap<Integer, Integer> adjCnt, BufferedWriter out) throws IOException
	{
		out.write(String.valueOf(pos));
		for(Integer adj : adjCnt.keySet())
		{
			if(adjCnt.get(adj) == null)
				System.err.println("Currupted line");
			out.write("\t" + adj + "\t" + adjCnt.get(adj));
		}
		out.write("\n");
	}
	
	
	private static void updateMap(PriorityQueue<Map.Entry<BufferedReader, Integer>> sortedEntries,
			HashMap<BufferedReader, String> fileLineMap,
			Entry<BufferedReader, Integer> min) throws IOException
	{
		BufferedReader in = min.getKey();
		fileLineMap.remove(in);
		
		String line = null;
		if( (line = in.readLine()) == null)
			return;	
		fileLineMap.put(in, line);
		
		
		int val = Integer.valueOf(line.split("\t")[0]);
		Map<BufferedReader, Integer> newMin = new HashMap<BufferedReader, Integer>();
		newMin.put(in, val);
	
		sortedEntries.addAll(newMin.entrySet());
	}
	
	
	private static void validate(File file) throws IOException
	{/*
		BufferedReader in = new BufferedReader(new FileReader(file));
		String line = null;
		Set<Integer> ids = new HashSet<Integer>();
		
		while((line = in.readLine()) != null)
			ids.add(Integer.valueOf(line.split("\t")[0]));
		in.close();
		
		in = new BufferedReader(new FileReader(file));
		while((line = in.readLine()) != null)
		{
			String[] splitLine = line.split("\t");
			for(int i = 1; i < splitLine.length; i+=2)
			{
				int id = Integer.valueOf(splitLine[i]);
				if(!ids.contains(id))
					System.err.println("Err: miss id!!!!!!!!!!" + file.getName());
			}
		}
		in.close();
		
		System.out.println("validated: " + file.getName());*/
	}
}