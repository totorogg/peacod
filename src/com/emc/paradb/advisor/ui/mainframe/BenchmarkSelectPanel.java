package com.emc.paradb.advisor.ui.mainframe;

import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Graphics;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.ButtonGroup;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JRadioButton;
import javax.swing.JTabbedPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.UIManager;
import javax.swing.UnsupportedLookAndFeelException;

import com.emc.paradb.advisor.algorithm.AlgorithmFactory;
import com.emc.paradb.advisor.controller.Controller;
import com.emc.paradb.advisor.controller.EvaluateController;
import com.emc.paradb.advisor.controller.PrepareController;
import com.emc.paradb.advisor.data_loader.DBData;
import com.emc.paradb.advisor.data_loader.DataLoader;
import com.emc.paradb.advisor.data_loader.PostgreSQLLoader;
import com.emc.paradb.advisor.plugin.KeyValuePair;
import com.emc.paradb.advisor.plugin.PlugInterface;
import com.emc.paradb.advisor.workload_loader.Transaction;
import com.emc.paradb.advisor.workload_loader.Workload;
import com.emc.paradb.advisor.workload_loader.WorkloadLoader;
import com.sun.java.swing.plaf.windows.WindowsLookAndFeel;

public class BenchmarkSelectPanel extends JTabbedPane implements ActionListener
{
	private JPanel benchmarkSelectPanel = new JPanel();
	private Box bmBox = Box.createVerticalBox();
	
	private JComboBox bmComboBox = null;
	private String[] bmString = {"TPC-C", "TPC-E", "TPC-W", "TATP"};

	private JComboBox dbComboBox = null;
	private String[] dbString = {"PostgreSQL"};
	
	private JTextField dataSetText = new JTextField("10");
	
	private JTextField nodeCountText = new JTextField("7");
	
	private JTextField workloadText = new JTextField("1000");
	
	private JButton evaluateButton = new JButton("Evaluate");
	private JButton recommendButton = new JButton("Recommend");
	
	private JProgressBar loadProgress = new JProgressBar(0, 100);
	
	public BenchmarkSelectPanel()
	{
		benchmarkSelectPanel.setLayout(new BoxLayout(benchmarkSelectPanel, BoxLayout.Y_AXIS));
		benchmarkSelectPanel.setPreferredSize(new Dimension(145, ABORT));
		benchmarkSelectPanel.setBorder(BorderFactory.createEmptyBorder(0, 5, 0, 5));
		
		bmComboBox = new JComboBox(bmString);
		bmComboBox.setSelectedIndex(0);
		bmBox.add(Box.createVerticalStrut(10));
		bmBox.add(bmComboBox);
		
		dbComboBox = new JComboBox(dbString);
		dbComboBox.setSelectedIndex(0);
		bmBox.add(Box.createVerticalStrut(10));
		bmBox.add(dbComboBox);
		
		bmBox.add(Box.createVerticalStrut(5));
		JTextArea workloadLabel = new JTextArea("Workload");
		workloadLabel.setBackground(null);
		bmBox.add(workloadLabel);
		bmBox.add(Box.createVerticalStrut(2));
		workloadText.setHorizontalAlignment(JTextField.RIGHT);
		workloadText.setMargin(new Insets(2, 5, 2, 5));
		Box workloadBox = Box.createHorizontalBox();
		workloadBox.add(workloadText);
		workloadBox.add(Box.createHorizontalStrut(10));
		workloadBox.add(new JLabel("Xacts"));
		bmBox.add(workloadBox);
		
		bmBox.add(Box.createVerticalStrut(5));
		JTextArea dataSetLabel = new JTextArea("DataSet Size");
		dataSetLabel.setBackground(null);
		bmBox.add(dataSetLabel);
		bmBox.add(Box.createVerticalStrut(2));
		Box dataSetBox = Box.createHorizontalBox();
		dataSetText.setHorizontalAlignment(JTextField.RIGHT);
		dataSetText.setMargin(new Insets(2, 5, 2, 5));
		dataSetBox.add(dataSetText);
		dataSetBox.add(Box.createHorizontalStrut(10));
		dataSetBox.add(new JLabel("GB"));
		bmBox.add(dataSetBox);
		
		bmBox.add(Box.createVerticalStrut(5));
		JTextArea nodeLabel = new JTextArea("Node Number");
		nodeLabel.setBackground(null);
		bmBox.add(nodeLabel);
		bmBox.add(Box.createVerticalStrut(2));
		nodeCountText.setHorizontalAlignment(JTextField.RIGHT);
		nodeCountText.setMargin(new Insets(2, 5, 2, 5));
		bmBox.add(nodeCountText);
		
		bmBox.add(Box.createVerticalStrut(5));
			
		Box buttonBox = Box.createHorizontalBox();
		
		loadProgress.setString("progress...");
		loadProgress.setStringPainted(true);
		bmBox.add(Box.createVerticalStrut(10));
		bmBox.add(loadProgress);
		
		bmBox.add(Box.createVerticalStrut(10));
		buttonBox.add(evaluateButton);
		evaluateButton.setMargin(new Insets(2,3,2,3));
		buttonBox.add(Box.createHorizontalStrut(5));
		buttonBox.add(recommendButton);
		recommendButton.setMargin(new Insets(2,3,2,3));
		buttonBox.add(Box.createHorizontalGlue());
		bmBox.add(buttonBox);
		bmBox.add(Box.createVerticalStrut(15));
		benchmarkSelectPanel.add(bmBox);
		this.add(benchmarkSelectPanel, "Benchmarks");
		
		evaluateButton.addActionListener(this);
		recommendButton.addActionListener(this);
	}

	@Override
	public void actionPerformed(ActionEvent arg0) 
	{
		// TODO Auto-generated method stub
		if(arg0.getSource() == evaluateButton)
		{
			new Thread()
			{
				public void run()
				{
					String selectedDB = dbComboBox.getSelectedItem().toString();
					String selectedBM = bmComboBox.getSelectedItem().toString();
					int nodes = Integer.valueOf(nodeCountText.getText().toString());
					PrepareController.start(selectedDB, selectedBM, nodes,loadProgress);
				}
			}.start();
		}
		else if(arg0.getSource() == recommendButton)
		{
			new Thread()
			{
				public void run()
				{
					int nodes = Integer.valueOf(nodeCountText.getText().toString());
					EvaluateController.recommend(nodes);
				}
			}.start();
		}
	}
}