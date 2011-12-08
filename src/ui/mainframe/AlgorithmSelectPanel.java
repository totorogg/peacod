package ui.mainframe;

import java.awt.GridLayout;
import java.awt.event.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTabbedPane;

import plugin.*;


public class AlgorithmSelectPanel extends JTabbedPane
{
	private JPanel algorithmSelectPanel = new JPanel();
	
	private JCheckBox algorithm1 = new JCheckBox("Algorithm1");
	private JCheckBox algorithm2 = new JCheckBox("Algorithm1");
	private JCheckBox algorithm3 = new JCheckBox("Algorithm1");
	private JCheckBox algorithm4 = new JCheckBox("Algorithm1");
	private JCheckBox algorithm5 = new JCheckBox("Algorithm1");
	
	private JButton config1 = new JButton("Config");
	private JButton config2 = new JButton("Config");
	private JButton config3 = new JButton("Config");
	private JButton config4 = new JButton("Config");
	private JButton config5 = new JButton("Config");
	
	public AlgorithmSelectPanel(){
		
		this.add(algorithmSelectPanel, "Algorithms");
		
		algorithmSelectPanel.setLayout(new GridLayout(8, 2, 10, 20));
		
		algorithmSelectPanel.add(new JLabel(""));
		algorithmSelectPanel.add(new JLabel(""));
		
		algorithmSelectPanel.add(algorithm1);
		algorithmSelectPanel.add(algorithm1);
		algorithmSelectPanel.add(config1);
		algorithmSelectPanel.add(algorithm2);
		algorithmSelectPanel.add(config2);
		algorithmSelectPanel.add(algorithm3);
		algorithmSelectPanel.add(config3);
		algorithmSelectPanel.add(algorithm4);
		algorithmSelectPanel.add(config4);
		algorithmSelectPanel.add(algorithm5);
		algorithmSelectPanel.add(config5);
		
		config1.addActionListener(new ActionListener(){
			
			public void actionPerformed(ActionEvent e){	
				
				PluginManager.loadPlugin();
				List<PlugInterface> pluginInterfaces = PluginManager
						.getInterfaces();

				for (PlugInterface aInterface : pluginInterfaces) {
					HashMap<String, String> tableKeyMap = aInterface
							.defaultAlgorithm();

					for (String tableName : tableKeyMap.keySet()) {
						System.out.println(tableName + ": "
								+ tableKeyMap.get(tableName));
					}
				}
			}
		});
		
	}

}