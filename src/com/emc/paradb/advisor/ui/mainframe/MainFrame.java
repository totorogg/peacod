package com.emc.paradb.advisor.ui.mainframe;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Insets;
import java.util.Enumeration;
import java.util.Hashtable;

import javax.swing.JFrame;
import javax.swing.JSplitPane;
import javax.swing.UIManager;
import javax.swing.UnsupportedLookAndFeelException;

import com.emc.paradb.advisor.ui.algorithm_panel.AlgorithmSelectPanel;
import com.sun.java.swing.plaf.windows.WindowsLookAndFeel;


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
		try {
			
			UIManager.setLookAndFeel(WindowsLookAndFeel.class.getName());

		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedLookAndFeelException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		UIManager.put("Panel.background", Color.WHITE);
		
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