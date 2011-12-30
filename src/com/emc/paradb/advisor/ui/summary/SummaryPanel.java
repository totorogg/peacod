package com.emc.paradb.advisor.ui.summary;

import java.awt.BorderLayout;
import java.util.ArrayList;
import java.util.List;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.JLabel;
import javax.swing.JPanel;


public class SummaryPanel extends JPanel
{
	private JLabel workloadChart = new JLabel("building");
	private Box box = Box.createHorizontalBox();
	
	public SummaryPanel()
	{
		this.setLayout(new BorderLayout());
		this.add(new JLabel("description: Summarize the coparison among algorithms"), BorderLayout.NORTH);
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