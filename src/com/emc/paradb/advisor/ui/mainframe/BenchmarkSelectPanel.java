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
	
	private JTextField nodeCountText = new JTextField("5");
	
	private JTextField workloadText = new JTextField("100");
	
	private JButton startButton = new JButton("Start");
	private JButton stopButton = new JButton("Stop");
	
	private JProgressBar loadProgress = new JProgressBar(0, 100);
	
	public BenchmarkSelectPanel()
	{

		
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
		workloadBox.add(new JLabel("Xact"));
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
		

		bmBox.add(Box.createVerticalStrut(10));
		
		Box buttonBox = Box.createHorizontalBox();
	
		loadProgress.setString("progress...");
		loadProgress.setStringPainted(true);
		bmBox.add(Box.createVerticalStrut(10));
		bmBox.add(loadProgress);

		bmBox.add(Box.createVerticalStrut(10));
		buttonBox.add(startButton);
		buttonBox.add(Box.createHorizontalStrut(10));
		buttonBox.add(stopButton);
		buttonBox.add(Box.createHorizontalGlue());
		bmBox.add(buttonBox);
		
		benchmarkSelectPanel.add(bmBox);
		this.add(benchmarkSelectPanel, "Benchmarks");
		
		startButton.addActionListener(this);
	}

	@Override
	public void actionPerformed(ActionEvent arg0) 
	{
		// TODO Auto-generated method stub
		new Thread()
		{
			public void run()
			{
				String selectedDB = dbComboBox.getSelectedItem().toString();
				String selectedBM = bmComboBox.getSelectedItem().toString();
				
				Controller.start(selectedDB, selectedBM, loadProgress);
			}
		}.start();
	}

	
	
}