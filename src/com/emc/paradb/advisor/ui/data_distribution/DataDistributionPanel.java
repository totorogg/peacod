package com.emc.paradb.advisor.ui.data_distribution;

import javax.swing.JPanel;

import java.awt.BorderLayout;
import java.awt.Font;
import java.util.ArrayList;
import java.util.List;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.ImageIcon;
import javax.swing.JLabel;
import javax.swing.JTextArea;
import javax.swing.JTextField;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;

import com.emc.paradb.advisor.controller.EvaluateController;

public class DataDistributionPanel extends JPanel
{
	private JLabel dataChart = null;
	private Box box = Box.createHorizontalBox();
	
	public DataDistributionPanel()
	{
		this.setLayout(new BorderLayout());

		JTextArea description = new JTextArea("DESCRIPTION: It describes how even data is distributed on each nodes.\n");
		description.setLineWrap(true);
		
		this.add(description, BorderLayout.NORTH);
		this.add(box, BorderLayout.CENTER);
		this.setBorder(BorderFactory.createEtchedBorder());
		
		
		EvaluateController.RegisterDataDistributionCB(new DataDistributionCB()
		{
			public void draw(List<Long> data)
			{
				box.removeAll();
				
				dataChart = DataChart.createChart(data);
				box.add(Box.createHorizontalGlue());
				box.add(dataChart);
				box.add(Box.createHorizontalGlue());
			}
		});
	}
}


class DataChart 
{
	public static JLabel createChart(List<Long> tuples){

		DefaultCategoryDataset categoryDataset = new DefaultCategoryDataset();
		String table = "";
		for (int i = 0; i < tuples.size(); i ++){
			categoryDataset.setValue(tuples.get(i), ""+i, table);
		}

		JFreeChart chart = ChartFactory.createBarChart("", // Title
				"Node", // X-Axis label
				"Data (Tuples)", // Y-Axis label
				categoryDataset, // Dataset, 
				PlotOrientation.VERTICAL, false, true, false);
		
		JLabel lb = new JLabel();
		lb.setIcon(new ImageIcon(chart.createBufferedImage(400,180)));
		
		return lb;
	}
}
