package com.emc.paradb.advisor.ui.summary;

/**
 * summary call back interface
 * this class needs to be defined by summary panel and registered in controller
 * the controller will call the registered call back draw() function 
 * after compare routine is finished
 * @author xpan
 *
 */
public interface SummaryCB
{
	public void drawSummaryTable(Object[][] data, Object[] columnNames,  
			String title, boolean append);
}