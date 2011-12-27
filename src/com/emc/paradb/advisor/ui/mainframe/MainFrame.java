package com.emc.paradb.advisor.ui.mainframe;

import java.awt.BorderLayout;

import javax.swing.JFrame;
import javax.swing.JSplitPane;

import com.emc.paradb.advisor.ui.algorithm_panel.AlgorithmSelectPanel;


public class MainFrame extends JFrame
{
	private static DisplayPanel displayPanel; 
	private static BenchmarkSelectPanel benchmarkSelectPanel;
	private static AlgorithmSelectPanel algorithmSelectPanel;
	private static JSplitPane splitPane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, true);
	public static void main(String[] argv)
	{	
		MainFrame mainFrame = new MainFrame();
		mainFrame.setVisible(true);
	}
	
	public MainFrame()
	{	
		displayPanel = new DisplayPanel();
		algorithmSelectPanel = new AlgorithmSelectPanel();
		benchmarkSelectPanel = new BenchmarkSelectPanel();
		
		this.setSize(900, 400);
		this.setTitle("PEACOD");
		this.setLayout(new BorderLayout());
		this.setDefaultCloseOperation(EXIT_ON_CLOSE);
		
		splitPane.setLeftComponent(displayPanel);
		splitPane.setRightComponent(algorithmSelectPanel);
		splitPane.setDividerSize(5);
		
		this.add(splitPane, BorderLayout.CENTER);
		this.add(benchmarkSelectPanel, BorderLayout.WEST);
	}
}