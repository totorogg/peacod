package com.emc.paradb.advisor.ui.algorithm_panel;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.List;

import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;


public class AlgorithmSetting extends JDialog implements ActionListener
{
	private JPanel settingPanel = null;
	private JButton ok = null;
	private JButton no = null;
	private JTextField input = null;
	private static List<JTextField> textList = null;
	
	public AlgorithmSetting(JFrame frame, boolean modal, String myMessage)
	{
		super(frame, modal);
		this.setLayout(new BorderLayout());
		this.setPreferredSize(new Dimension(500,200));
		this.setMinimumSize(new Dimension(500,100));
	}

	@Override
	public void actionPerformed(ActionEvent arg0) {
		// TODO Auto-generated method stub
		if(arg0.getSource() == ok)
		{
			
			setVisible(false);
		}
	}
	private void setContent(List<String[]> paraList)
	{
		int line = paraList.size();
		settingPanel = new JPanel();
		settingPanel.setLayout(new BoxLayout(settingPanel, BoxLayout.Y_AXIS));
		textList = new ArrayList<JTextField>();
		
		for(String[] aPara : paraList)
		{
			Box paraBox = Box.createHorizontalBox();	
			paraBox.setMaximumSize(new Dimension(400, 20));
			paraBox.setPreferredSize(new Dimension(400, 20));
			
			Box nameBox = Box.createHorizontalBox();
			nameBox.add(new JTextArea(aPara[0]));
			nameBox.setPreferredSize(new Dimension(80, 20));
			nameBox.setMaximumSize(new Dimension(80, 20));
			paraBox.add(nameBox);
			
			paraBox.add(Box.createHorizontalStrut(20));
			
			input = new JTextField();
			textList.add(input);
			Box inputBox = Box.createHorizontalBox();
			inputBox.add(input);
			inputBox.setPreferredSize(new Dimension(100, 20));
			inputBox.setMinimumSize(new Dimension(50, 20));
			paraBox.add(inputBox);
			
			paraBox.add(Box.createHorizontalStrut(20));
			
			JTextArea descrip = new JTextArea("descrip: " + aPara[2]);
			//descrip.setAutoscrolls(true);
			Box descripBox = Box.createHorizontalBox();
			descripBox.add(descrip);
			descripBox.setPreferredSize(new Dimension(300, 20));
			descripBox.setMaximumSize(new Dimension(300, 20));
			paraBox.add(descripBox);
			
			settingPanel.add(paraBox);
		}
		settingPanel.add(Box.createVerticalGlue());
		
		
		this.add(new JScrollPane(settingPanel), BorderLayout.CENTER);
		ok = new JButton("Apply");
		ok.addActionListener(this);
		this.add(ok, BorderLayout.SOUTH);
		
		this.setVisible(true);
	}
	
	public List<String> getSetting()
	{
		List<String> results = new ArrayList<String>();
		for(JTextField input : textList)
			results.add(input.getText());
		
		return results;
	}
	
	public static void disPlaySetting(List<String[]> paraList)
	{
		JFrame frame = new JFrame();
		frame.setSize(800, 380);
		frame.setLayout(new BorderLayout());
		frame.setDefaultCloseOperation(EXIT_ON_CLOSE);
		AlgorithmSetting settingDialog = new AlgorithmSetting(new JFrame(), true, "setting");
		settingDialog.setContent(paraList);
		
		for(int i = 0; i < paraList.size(); i++)
			paraList.get(i)[1] += textList.get(i).getText();
	}
}