package com.emc.paradb.advisor.ui.mainframe;

import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.Box;
import javax.swing.ButtonGroup;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JRadioButton;
import javax.swing.JTabbedPane;
import javax.swing.JTextField;

import com.emc.paradb.advisor.data_loader.DataLoader;
import com.emc.paradb.advisor.workload_loader.WorkloadLoader;

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
	
	
	private JButton startButton = new JButton("Start");
	private JButton stopButton = new JButton("Stop");
	
	private JProgressBar loadProgress = new JProgressBar(0, 100);
	
	public BenchmarkSelectPanel()
	{
		Box.createVerticalStrut(100);

		bmComboBox = new JComboBox(bmString);
		bmComboBox.setSelectedIndex(0);
		bmBox.add(Box.createVerticalStrut(20));
		bmBox.add(bmComboBox);
		
		dbComboBox = new JComboBox(dbString);
		dbComboBox.setSelectedIndex(0);
		bmBox.add(Box.createVerticalStrut(20));
		bmBox.add(dbComboBox);
		
		bmBox.add(Box.createVerticalStrut(20));
		bmBox.add(new JLabel("DataSet(GB)"));
		bmBox.add(dataSetText);

		bmBox.add(Box.createVerticalStrut(20));
		bmBox.add(new JLabel("Node #"));
		bmBox.add(nodeCountText);
		
		bmBox.add(Box.createVerticalStrut(50));
		
		Box buttonBox = Box.createHorizontalBox();
		buttonBox.add(startButton);
		buttonBox.add(Box.createHorizontalStrut(10));
		buttonBox.add(stopButton);
		bmBox.add(buttonBox);
		
		bmBox.add(Box.createVerticalStrut(10));
		bmBox.add(loadProgress);
		
		benchmarkSelectPanel.add(bmBox);
		this.add(benchmarkSelectPanel, "Benchmarks");
		
		startButton.addActionListener(this);
	}

	@Override
	public void actionPerformed(ActionEvent arg0) 
	{
		// TODO Auto-generated method stub
		new Thread(){
			public void run()
			{
				try
				{
					String selectedDB = dbComboBox.getSelectedItem().toString();
					String selectedBM = bmComboBox.getSelectedItem().toString();
					
					//load workload for the selected benchmark
					WorkloadLoader workloadLoader = new WorkloadLoader(selectedBM);
					workloadLoader.load();
					
					while(true)
					{
						int progress = (int)(workloadLoader.getProgress() * 100);
						loadProgress.setValue(progress);
						if(progress > 99)
							break;
						this.sleep(10);
					}

					//load data from the selected data source
					DataLoader dataLoader = new DataLoader(selectedDB, selectedBM);
					dataLoader.load();
					

				}
				catch(Exception e)
				{
					System.out.println(e.getMessage());
					e.getStackTrace();
				}
				
				
			}
		}.start();	
	}

	
	
}