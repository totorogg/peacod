package com.emc.paradb.advisor.workload_loader;

import java.io.StringReader;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;


public class QueryAnalyzer {

	public static Statement analyze(String sql){
		CCJSqlParserManager parserManager = new CCJSqlParserManager();
		try {
			return parserManager.parse(new StringReader(sql));
		} catch (JSQLParserException e) {
			return null;
		}
	}
}