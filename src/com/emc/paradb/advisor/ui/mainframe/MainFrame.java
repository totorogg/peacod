package com.emc.paradb.advisor.ui.mainframe;

import java.awt.BorderLayout;

import javax.swing.JFrame;


public class MainFrame extends JFrame
{
	
	private static DisplayPanel displayPanel; 
	private static BenchmarkSelectPanel benchmarkSelectPanel;
	private static AlgorithmSelectPanel algorithmSelectPanel;
	
	public static void main(String[] argv){
		
		MainFrame mainFrame = new MainFrame();
		mainFrame.setVisible(true);
	}
	
	public MainFrame(){
		
		displayPanel = new DisplayPanel();
		algorithmSelectPanel = new AlgorithmSelectPanel();
		benchmarkSelectPanel = new BenchmarkSelectPanel();
		
		this.setSize(800, 400);
		this.setTitle("Sharding Advisor");
		this.setLayout(new BorderLayout());
		this.setDefaultCloseOperation(EXIT_ON_CLOSE);
		
		this.add(displayPanel, BorderLayout.CENTER);
		this.add(algorithmSelectPanel, BorderLayout.EAST);
		this.add(benchmarkSelectPanel, BorderLayout.WEST);
		
	}
}