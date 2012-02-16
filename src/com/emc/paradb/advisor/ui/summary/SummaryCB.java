package com.emc.paradb.advisor.ui.summary;


public interface SummaryCB
{
	public void drawSummaryTable(Object[][] data, Object[] columnNames,  
			String title, boolean append);
}