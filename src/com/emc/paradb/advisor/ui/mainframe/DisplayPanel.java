package com.emc.paradb.advisor.ui.mainframe;

import java.awt.Dimension;

import javax.swing.JPanel;
import javax.swing.JTabbedPane;

import com.emc.paradb.advisor.ui.data_distribution.DataDistributionPanel;
import com.emc.paradb.advisor.ui.data_migration.DataMigrationPanel;
import com.emc.paradb.advisor.ui.summary.SummaryPanel;
import com.emc.paradb.advisor.ui.transaction_distribution.TransactionDistributionPanel;
import com.emc.paradb.advisor.ui.workload_distribution.WorkloadDistributionPanel;


public class DisplayPanel extends JTabbedPane
{
	DataDistributionPanel dataDistributionPanel = new DataDistributionPanel();
	WorkloadDistributionPanel workloadDistributionPanel = new WorkloadDistributionPanel();
	TransactionDistributionPanel tranDistPanel = new TransactionDistributionPanel();
	DataMigrationPanel dataMigrationPanel = new DataMigrationPanel();
	SummaryPanel summaryPanel = new SummaryPanel();
	
	public DisplayPanel()
	{
		this.setPreferredSize(new Dimension(600, 400));
		this.setMinimumSize(new Dimension(300, 400));
		
		this.add(dataDistributionPanel, "Data Distribution");
		this.add(workloadDistributionPanel, "Workload Distribution");
		this.add(tranDistPanel, "# of Distributed Xacts");
		//this.add(dataMigrationPanel, "Data Migration");
		this.add(summaryPanel, "Summary");
	}
}