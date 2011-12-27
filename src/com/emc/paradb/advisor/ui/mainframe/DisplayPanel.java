package com.emc.paradb.advisor.ui.mainframe;

import java.awt.Dimension;

import javax.swing.JPanel;
import javax.swing.JTabbedPane;

import com.emc.paradb.advisor.ui.data_distribution.DataDistributionPanel;
import com.emc.paradb.advisor.ui.data_migration.DataMigrationPanel;
import com.emc.paradb.advisor.ui.transaction_distribution.TransactionDistributionPanel;
import com.emc.paradb.advisor.ui.workload_distribution.WorkloadDistributionPanel;


public class DisplayPanel extends JTabbedPane
{
	DataDistributionPanel dataDistributionPanel = new DataDistributionPanel();
	WorkloadDistributionPanel workloadDistributionPanel = new WorkloadDistributionPanel();
	TransactionDistributionPanel tranDistPanel = new TransactionDistributionPanel();
	DataMigrationPanel dataMigrationPanel = new DataMigrationPanel();
	
	public DisplayPanel(){
		this.setPreferredSize(new Dimension(500, 400));
		this.setMinimumSize(new Dimension(300, 400));
		
		this.add(dataDistributionPanel, "Data Dist.");
		this.add(workloadDistributionPanel, "Workload Dist.");
		this.add(tranDistPanel, "No. of Dist. Tran.");
		this.add(dataMigrationPanel, "Data Migration");
	}
}