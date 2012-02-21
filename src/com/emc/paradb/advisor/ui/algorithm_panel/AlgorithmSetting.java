package com.emc.paradb.advisor.ui.algorithm_panel;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.List;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JTextArea;


public class AlgorithmSetting extends JDialog implements ActionListener
{
	private JPanel settingPanel = null;
	private JButton ok = null;
	private JButton no = null;
	private JTextArea input = null;
	
	public AlgorithmSetting(JFrame frame, boolean modal, String myMessage)
	{
		super(frame, modal);
		this.setSize(300,200);
		settingPanel = new JPanel();
		settingPanel.setLayout(new BorderLayout());
		input = new JTextArea("11");
		settingPanel.add(input, BorderLayout.NORTH);
		
		ok = new JButton("ok");
		ok.addActionListener(this);
		settingPanel.add(ok, BorderLayout.SOUTH);
		
		this.add(settingPanel);
		setVisible(true);
	}

	@Override
	public void actionPerformed(ActionEvent arg0) {
		// TODO Auto-generated method stub
		if(arg0.getSource() == ok)
		{
			setVisible(false);
		}
	}
	
	public String getSetting()
	{
		return input.getText();
	}
	
	public static void disPlaySetting(List<String[]> paraList)
	{
		JFrame frame = new JFrame();
		frame.setSize(800, 380);
		frame.setLayout(new BorderLayout());
		frame.setDefaultCloseOperation(EXIT_ON_CLOSE);
		AlgorithmSetting settingDialog = new AlgorithmSetting(new JFrame(), true, "setting");
		
		System.out.println(settingDialog.getSetting());	
	}
}