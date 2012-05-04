package com.emc.paradb.advisor.ui.algorithm_panel;

import java.awt.Dimension;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JPanel;
import javax.swing.event.TableModelEvent;
import javax.swing.event.TableModelListener;
import javax.swing.table.TableModel;

import com.emc.paradb.advisor.algorithm.AlgorithmFactory;
import com.emc.paradb.advisor.controller.AlgorithmController;
import com.emc.paradb.advisor.controller.Controller;
import com.emc.paradb.advisor.controller.EvaluateController;


/**
 * controller of algorithm table, function include
 * insert algorithms into table
 * evaluate selected algorithms in the table
 * compare selected algorithms in the table
 * @author xpan
 *
 */
public class AlgorithmTableControlPanel extends JPanel
{
	private JButton load = new JButton("Select");
	private JButton evaluate = new JButton("Evaluate");
	private JButton compare = new JButton("Compare");
	
	private AlgorithmTable algorithmTable = null;

	
	public AlgorithmTableControlPanel(){
		
		this.setLayout(new BoxLayout(this, BoxLayout.Y_AXIS));
		this.setBorder(BorderFactory.createEtchedBorder());
		Box box = Box.createHorizontalBox();
		
		
		load.setMaximumSize(new Dimension(ABORT, 20));
		load.setMargin(new Insets(2,3,2,3));
		evaluate.setMaximumSize(new Dimension(ABORT, 20));
		evaluate.setMargin(new Insets(2,3,2,3));
		compare.setMaximumSize(new Dimension(ABORT, 20));
		compare.setMargin(new Insets(2,3,2,3));
		
		box.add(load);
		box.add(Box.createHorizontalStrut(5));
		box.add(evaluate);
		box.add(Box.createHorizontalStrut(5));
		box.add(compare);
		
		load.setPreferredSize(new Dimension(ABORT, 20));
		box.setSize(ABORT, 20);
		this.add(box);
	
		//register button click event handlers
		load.addActionListener(new ActionListener()
		{	
			public void actionPerformed(ActionEvent e)
			{
				AlgorithmController.loadAlgorithm();				
				algorithmTable.setData(AlgorithmFactory.getAlgorithms());
			}
		});
		
		/**
		 * to avoid long time blocking during evaluation routine
		 * we create a new thread for evalutation process
		 */
		evaluate.addActionListener(new ActionListener()
		{	
			public void actionPerformed(ActionEvent e)
			{
				new Thread()
				{
					public void run()
					{
						EvaluateController.evaluate();
					}
				}.start();		
			}
		});
		
		compare.addActionListener(new ActionListener()
		{	
			public void actionPerformed(ActionEvent e)
			{
				EvaluateController.compare();
			}
		});
		
		
	}
	
	public void registerTable(AlgorithmTable aTable){
		this.algorithmTable = aTable;
	}
	

}