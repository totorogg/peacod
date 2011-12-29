package com.emc.paradb.advisor.ui.algorithm_panel;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;

import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;


public class AlgorithmSelectPanel extends JTabbedPane
{
	private AlgorithmTableControlPanel atcPanel = null;
	private AlgorithmTable algorithmTable = null;
	private AlgorithmDescriptionPanel adPanel = null;
	public AlgorithmSelectPanel() 
	{	
		//create the description panel
		adPanel = new AlgorithmDescriptionPanel();
		
		//create the algorithm table
		algorithmTable = new AlgorithmTable();
		algorithmTable.registerDescriptionPanel(adPanel);
		JScrollPane tableSP= new JScrollPane(algorithmTable);
		tableSP.getViewport().setBackground(Color.WHITE);
		tableSP.setPreferredSize(new Dimension(200, 200));
		
		//create a control panel for the algorithmTable
		atcPanel = new AlgorithmTableControlPanel();	
		atcPanel.registerTable(algorithmTable);
		
		JPanel tabbedPanel = new JPanel();
		tabbedPanel.setPreferredSize(new Dimension(200, 400));
		tabbedPanel.setLayout(new BorderLayout());
		tabbedPanel.add(atcPanel, BorderLayout.NORTH);
		tabbedPanel.add(tableSP, BorderLayout.CENTER);
		tabbedPanel.add(adPanel, BorderLayout.SOUTH);
		
		this.add(tabbedPanel, "Algorithms");
		this.setPreferredSize(new Dimension(200, 400));
		this.setMinimumSize(new Dimension(150, 400));
	}
	
	public AlgorithmTable getAlgorithmTable()
	{
		return algorithmTable;
	}
	
	public AlgorithmTableControlPanel getAlgorithmControlPanel()
	{
		return atcPanel;
	}
}







