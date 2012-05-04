package com.emc.paradb.advisor.utils;

import java.util.HashMap;

/**
 * this class provides with a few static function to translate table name
 * when key word is used as table name in database, the name will be enclosed with "";
 * 
 * @author Xin
 *
 */
public class QueryPrepare
{
	private static HashMap<String, String> translateMap = new HashMap<String, String>();
	private static HashMap<String, String> untranslateMap = new HashMap<String, String>();
	static 
	{
		translateMap.put("user", "\"user\"");
		untranslateMap.put("\"user\"", "user");
	}
	public static String prepare(String table)
	{
		if(translateMap.get(table) == null)
			return table;
		else
			return translateMap.get(table);
	}
	
	public static String unPrepare(String table)
	{
		if(untranslateMap.get(table) == null)
			return table;
		else
			return untranslateMap.get(table);
	}
}