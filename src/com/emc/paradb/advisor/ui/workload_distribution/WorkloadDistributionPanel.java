package com.emc.paradb.advisor.ui.workload_distribution;

import java.awt.BorderLayout;
import java.awt.Font;
import java.util.ArrayList;
import java.util.List;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.ImageIcon;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextArea;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;

import com.emc.paradb.advisor.controller.EvaluateController;
import com.emc.paradb.advisor.ui.data_distribution.DataDistributionCB;


public class WorkloadDistributionPanel extends JPanel
{
	private JLabel workloadChart = null;
	private Box box = Box.createHorizontalBox();
	
	public WorkloadDistributionPanel(){
		this.setLayout(new BorderLayout());
		this.add(box, BorderLayout.CENTER);
		JTextArea description = new JTextArea("DESCRIPTION: The number of data accessed by transactions on each node");
		this.add(description, BorderLayout.NORTH);
		this.setBorder(BorderFactory.createEtchedBorder());
		
		EvaluateController.RegisterWorkloadDistributionCB(new WorkloadDistributionCB()
		{
			public void draw(List<Long> data)
			{
				box.removeAll();
				
				workloadChart = WorkloadChart.createChart(data);
				box.add(Box.createHorizontalGlue());
				box.add(workloadChart);
				box.add(Box.createHorizontalGlue());
			}
		});
	}
}
class WorkloadChart 
{	
	public static JLabel createChart(List<Long> tuples){
		
		DefaultCategoryDataset categoryDataset = new DefaultCategoryDataset();
		String table = "";
		for (int i = 0; i < tuples.size(); i ++){
			categoryDataset.setValue(tuples.get(i), ""+i, table);
		}
		JFreeChart chart = ChartFactory.createBarChart("Workload Distribution", // Title
				"Node", // X-Axis label
				"Data (Tuples)", // Y-Axis label
				categoryDataset, // Dataset, 
				PlotOrientation.VERTICAL, false, true, false);
		
		JLabel lb = new JLabel();
		lb.setIcon(new ImageIcon(chart.createBufferedImage(400,180)));
		
		return lb;
	}
}


