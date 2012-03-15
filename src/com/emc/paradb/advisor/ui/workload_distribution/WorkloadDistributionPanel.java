package com.emc.paradb.advisor.ui.workload_distribution;

import java.awt.BorderLayout;
import java.awt.Font;
import java.util.ArrayList;
import java.util.HashMap;
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

import com.emc.paradb.advisor.controller.DisplayController;

public class WorkloadDistributionPanel extends JPanel {
	private JLabel workloadChart = null;
	private Box box = Box.createHorizontalBox();

	public WorkloadDistributionPanel() {
		this.setLayout(new BorderLayout());
		this.add(box, BorderLayout.CENTER);
		JTextArea description = new JTextArea(
				"DESCRIPTION: \n This metric evaluates how uniformly the data accesses by the workload are distributed across partitions. "
						+ "In a single scheme's descriptive representation, each node's total transactions be executed will be showed. In the comparasion representation, "
						+ "the metric is represented by the standard variance normalized to be a value between zero and one. \n");

		this.add(description, BorderLayout.NORTH);
		this.setBorder(BorderFactory.createEtchedBorder());

		DisplayController
				.registerWorkloadDistributionCB(new WorkloadDistributionCB() {
					public void draw(List<Long> data) {
						box.removeAll();

						workloadChart = WorkloadChart.createChart(data);
						box.add(Box.createHorizontalGlue());
						box.add(workloadChart);
						box.add(Box.createHorizontalGlue());
						box.updateUI();
						box.validate();
					}

					@Override
					public void draw(HashMap<String, Float> wDVarMap) {
						// TODO Auto-generated method stub
						box.removeAll();

						workloadChart = WorkloadChart.createChart(wDVarMap);
						box.add(Box.createHorizontalGlue());
						box.add(workloadChart);
						box.add(Box.createHorizontalGlue());
						box.repaint();
						box.validate();
					}
				});
	}
}

class WorkloadChart {
	public static JLabel createChart(List<Long> tuples) {

		DefaultCategoryDataset categoryDataset = new DefaultCategoryDataset();
		String table = "";
		for (int i = 0; i < tuples.size(); i++) {
			categoryDataset.setValue(tuples.get(i), "" + i, table);
		}
		JFreeChart chart = ChartFactory.createBarChart("Workload Distribution", // Title
				"Node", // X-Axis label
				"Visit Frequency", // Y-Axis label
				categoryDataset, // Dataset,
				PlotOrientation.VERTICAL, false, true, false);

		JLabel lb = new JLabel();
		lb.setIcon(new ImageIcon(chart.createBufferedImage(400, 180)));

		return lb;
	}

	public static JLabel createChart(HashMap<String, Float> wDVarMap) {

		DefaultCategoryDataset categoryDataset = new DefaultCategoryDataset();
		String table = "";
		for (String algorithm : wDVarMap.keySet()) {
			int index = algorithm.lastIndexOf(".");
			String name = algorithm.substring(index + 1);
			categoryDataset.setValue(wDVarMap.get(algorithm), "", name);
		}

		JFreeChart chart = ChartFactory.createBarChart("Distribution Variance", // Title
				"Algorithms", // X-Axis label
				"Variance", // Y-Axis label
				categoryDataset, // Dataset,
				PlotOrientation.VERTICAL, false, true, false);

		JLabel lb = new JLabel();
		lb.setIcon(new ImageIcon(chart.createBufferedImage(400, 180)));

		return lb;
	}
}
