package com.emc.paradb.advisor.ui.data_migration;

import java.awt.BorderLayout;
import java.util.ArrayList;
import java.util.List;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextArea;



public class DataMigrationPanel extends JPanel
{
	private JLabel workloadChart = new JLabel("building");
	private Box box = Box.createHorizontalBox();
	
	public DataMigrationPanel()
	{
		this.setLayout(new BorderLayout());
		this.add(
				new JTextArea("DESCRIPTION: \n This metric indicated the amount of data to be migrated among nodes in case of database re-partitioning. Here, we will assume the scenatio where database re-partitioning is triggered for the load re-balance of the system with one new node added. "), BorderLayout.NORTH);
		this.add(box, BorderLayout.CENTER);
		this.setBorder(BorderFactory.createEtchedBorder());
		
		List<Integer> data = new ArrayList<Integer>();
		
		for(int i = 0; i < 10; i++){
			data.add(10);
		}
		setChart(data);
	}
	
	public void setChart(List<Integer> data){
		
		box.add(Box.createHorizontalGlue());
		box.add(workloadChart);
		box.add(Box.createHorizontalGlue());
	}
	
	
}












