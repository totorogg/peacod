package com.emc.paradb.advisor.ui.algorithm_panel;

import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.JButton;
import javax.swing.JPanel;
import javax.swing.event.TableModelEvent;
import javax.swing.event.TableModelListener;
import javax.swing.table.TableModel;

import com.emc.paradb.advisor.algorithm.AlgorithmFactory;
import com.emc.paradb.advisor.controller.Controller;



public class AlgorithmTableControlPanel extends JPanel
{
	private JButton load = new JButton("Load");
	private AlgorithmTable algorithmTable = null;

	
	public AlgorithmTableControlPanel(){
		this.setBorder(BorderFactory.createEtchedBorder());
		Box box = Box.createHorizontalBox();
		box.add(Box.createHorizontalStrut(180));
		box.add(load);
		
		load.setPreferredSize(new Dimension(ABORT, 20));
		box.setSize(ABORT, 20);
		this.add(box);
	
		load.addActionListener(new ActionListener()
		{	
			public void actionPerformed(ActionEvent e)
			{
				Controller.loadAlgorithm();				
				algorithmTable.setData(AlgorithmFactory.getAlgorithms());
			}
		});
	}
	
	public void registerTable(AlgorithmTable aTable){
		this.algorithmTable = aTable;
	}
	

}