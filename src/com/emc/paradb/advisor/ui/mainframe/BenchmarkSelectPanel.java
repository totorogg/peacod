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
	private String[] bmString = {"TPC-C", "EPINIONS", "TATP"};

	private JComboBox dbComboBox = null;
	private String[] dbString = {"PostgreSQL"};
	
	private JTextField dataSetText = new JTextField("10");
	
	private JTextField nodeCountText = new JTextField("7");
	
	private JTextField workloadText = new JTextField("1000");
	
	private JButton prepareButton = new JButton("Configure");
	
	private JProgressBar loadProgress = new JProgressBar(0, 100);
	
	public BenchmarkSelectPanel()
	{
		benchmarkSelectPanel.setLayout(new BoxLayout(benchmarkSelectPanel, BoxLayout.Y_AXIS));
		//benchmarkSelectPanel.setPreferredSize(new Dimension(140, ABORT));
		benchmarkSelectPanel.setPreferredSize(new Dimension((int) (MainFrame.scWidth * 0.175), ABORT));
		benchmarkSelectPanel.setBorder(BorderFactory.createEmptyBorder(0, 5, 0, 5));
		
		bmComboBox = new JComboBox(bmString);
		bmComboBox.setSelectedIndex(0);
		bmComboBox.addActionListener(new ActionListener()
		{

			@Override
			public void actionPerformed(ActionEvent arg0) 
			{
				// TODO Auto-generated method stub
				JComboBox cb = (JComboBox)arg0.getSource();
				int selectedIndex = cb.getSelectedIndex();
				switch(selectedIndex)
				{
				case 0:
					workloadText.setText(PrepareController.getTPCCBNUM());
					dataSetText.setText(PrepareController.getTPCCSize());
					break;
				case 1:
					workloadText.setText(PrepareController.getEPINIONBNUM());
					dataSetText.setText(PrepareController.getTATPSize());
					break;
				case 2:
					workloadText.setText(PrepareController.getTATPBNUM());
					dataSetText.setText(PrepareController.getEPINIONSize());
					break;
				}
			}
			
		});
		bmBox.add(Box.createVerticalStrut(10));
		bmBox.add(bmComboBox);
		
		
		dbComboBox = new JComboBox(dbString);
		dbComboBox.setSelectedIndex(0);
		bmBox.add(Box.createVerticalStrut(20));
		bmBox.add(dbComboBox);
		
		bmBox.add(Box.createVerticalStrut(20));
		JTextArea workloadLabel = new JTextArea("Workload Size");
		//JLabel workloadLabel = new JLabel("Workload");
		workloadLabel.setBackground(null);
		//workloadLabel.setAlignmentX(0.5f);
		bmBox.add(workloadLabel);
		bmBox.add(Box.createVerticalStrut(5));
		workloadText.setHorizontalAlignment(JTextField.RIGHT);
		workloadText.setMargin(new Insets(2, 5, 2, 5));
		Box workloadBox = Box.createHorizontalBox();
		workloadBox.add(workloadText);
		workloadBox.add(Box.createHorizontalStrut(20));
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

		dataSetBox.add(Box.createHorizontalStrut(20));
		dataSetBox.add(new JLabel("MB"));

		bmBox.add(dataSetBox);
		
		bmBox.add(Box.createVerticalStrut(5));
		JTextArea nodeLabel = new JTextArea("# Data Nodes");
		nodeLabel.setBackground(null);
		bmBox.add(nodeLabel);
		bmBox.add(Box.createVerticalStrut(2));
		nodeCountText.setHorizontalAlignment(JTextField.RIGHT);
		//nodeCountText.setPreferredSize(new Dimension(10, ABORT));
		nodeCountText.setMargin(new Insets(2, 5, 2, 5));
		bmBox.add(nodeCountText);
		
		bmBox.add(Box.createVerticalStrut(5));
			
		Box buttonBox = Box.createHorizontalBox();
		
		loadProgress.setString("progress...");
		loadProgress.setStringPainted(true);
		bmBox.add(Box.createVerticalStrut(10));
		bmBox.add(loadProgress);
		
		bmBox.add(Box.createVerticalStrut(10));
		buttonBox.add(prepareButton);
		buttonBox.add(Box.createHorizontalGlue());
		prepareButton.setMargin(new Insets(2,3,2,3));
		
		bmBox.add(buttonBox);
		bmBox.add(Box.createVerticalStrut(15));
		benchmarkSelectPanel.add(bmBox);
		this.add(benchmarkSelectPanel, "Benchmarks");
		
		prepareButton.addActionListener(this);
	}

	private String getWorkloadCount(String bm)
	{
		return null;
	}
	
	@Override
	public void actionPerformed(ActionEvent arg0) 
	{
		// TODO Auto-generated method stub
		if(arg0.getSource() == prepareButton)
		{
			new Thread()
			{
				public void run()
				{
					String selectedDB = dbComboBox.getSelectedItem().toString();
					String selectedBM = bmComboBox.getSelectedItem().toString();
					int nodes = Integer.valueOf(nodeCountText.getText().toString());
					int transactions = Integer.valueOf(workloadText.getText().toString());
					PrepareController.start(selectedDB, selectedBM, nodes, transactions, progressCB);
				}
			}.start();
		}
	}
	
	private ProgressCB progressCB = new ProgressCB(){

		@Override
		public void setProgress(int progress) {
			// TODO Auto-generated method stub
			loadProgress.setValue(progress);
		}

		@Override
		public void setState(String state) {
			// TODO Auto-generated method stub
			loadProgress.setString(state);
			loadProgress.setStringPainted(true);
		}
	};
}