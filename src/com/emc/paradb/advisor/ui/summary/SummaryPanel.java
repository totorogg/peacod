package com.emc.paradb.advisor.ui.summary;

import java.awt.BorderLayout;
import java.util.ArrayList;
import java.util.List;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.table.DefaultTableModel;

import com.emc.paradb.advisor.controller.DisplayController;


public class SummaryPanel extends JPanel
{
	private Box box = Box.createVerticalBox();
	Box tableBox = Box.createVerticalBox();
	private JTable summaryTable = null;
	
	public SummaryPanel()
	{
		this.setLayout(new BorderLayout());
		this.add(new JLabel("Description: Comparison summary among different partitioning schemes"), BorderLayout.NORTH);
		
		this.add(box, BorderLayout.CENTER);
		this.setBorder(BorderFactory.createEtchedBorder());
		
		box.add(tableBox);
		DisplayController.registerSummaryCB(new SummaryCB()
		{
			@Override
			public void drawSummaryTable(Object[][] data, Object[] columnNames,
					String title, boolean append) 
			{
				// TODO Auto-generated method stub
				DefaultTableModel dm = new DefaultTableModel();
				dm.setColumnIdentifiers(columnNames);
				dm.setDataVector(data, columnNames);
				
				summaryTable = new JTable();
				summaryTable.setModel(dm);
				
				if(!append)
					tableBox.removeAll();

				
				JScrollPane jsp = new JScrollPane(summaryTable);
				Box scrollBox = Box.createHorizontalBox();
				scrollBox.add(Box.createHorizontalStrut(40));
				scrollBox.add(jsp);
				scrollBox.add(Box.createHorizontalStrut(40));
				
				tableBox.add(Box.createVerticalStrut(10));
				tableBox.add(new JLabel(title));
				tableBox.add(scrollBox);
				tableBox.add(Box.createHorizontalGlue());
				tableBox.updateUI();
				tableBox.validate();
			}	
		});
	}
}

