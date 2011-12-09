package com.emc.paradb.advisor.ui.mainframe;

import javax.swing.JPanel;
import javax.swing.JTabbedPane;


public class DisplayPanel extends JTabbedPane
{
	
	JPanel dataDistribution = new JPanel();
	JPanel workloadDistribution = new JPanel();
	JPanel distTranCount = new JPanel();
	JPanel dataMigration = new JPanel();
	
	public DisplayPanel(){
		
		this.add(dataDistribution, "Data Dist.");
		this.add(workloadDistribution, "Workload Dist.");
		this.add(distTranCount, "No. of Dist. Tran.");
		this.add(dataMigration, "Data Migration");

	}
}