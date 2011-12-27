package com.emc.paradb.advisor.ui.algorithm_panel;

import java.awt.BorderLayout;
import java.awt.Dimension;

import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;


public class AlgorithmDescriptionPanel extends JPanel
{
	JTextArea description = new JTextArea("Description:\n");
	
	public AlgorithmDescriptionPanel()
	{
		description.setLineWrap(true);
		description.setBackground(null);
		this.setPreferredSize(new Dimension(200, 100));
		this.setLayout(new BorderLayout());
		this.add(description, BorderLayout.NORTH);
		this.setVisible(true);
	}
	
	public void setText(String text)
	{
		description.setText("Description:\n" + "  " + text);
	}
}