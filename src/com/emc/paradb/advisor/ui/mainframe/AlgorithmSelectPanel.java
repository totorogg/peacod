package com.emc.paradb.advisor.ui.mainframe;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.GridLayout;
import java.awt.event.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import javax.swing.AbstractCellEditor;
import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.DefaultCellEditor;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTable;
import javax.swing.UIManager;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableCellEditor;
import javax.swing.table.TableCellRenderer;

import com.emc.paradb.advisor.plugin.PlugInterface;
import com.emc.paradb.advisor.plugin.PluginManager;


public class AlgorithmSelectPanel extends JTabbedPane {


	public AlgorithmSelectPanel() {
		
		//create the algorithm table
		AlgorithmTable algorithmTable = new AlgorithmTable();
		JScrollPane jsp = new JScrollPane(algorithmTable);
		jsp.setPreferredSize(new Dimension(200, 300));
		
		//create a control panel for the algorithmTable
		AlgorithmTableControlPanel atcPanel = new AlgorithmTableControlPanel();
		atcPanel.registerTable(algorithmTable);
		
		JPanel tabbedPanel = new JPanel();
		tabbedPanel.setPreferredSize(new Dimension(200, 400));
		this.add(tabbedPanel, "Algorithms");
		
		tabbedPanel.setLayout(new BorderLayout());
		tabbedPanel.add(atcPanel, BorderLayout.NORTH);
		tabbedPanel.add(jsp, BorderLayout.CENTER);
	}

	public void display() {

	}
	
}



class AlgorithmTableControlPanel extends JPanel
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
		
		load.addActionListener(new ActionListener(){
			
			public void actionPerformed(ActionEvent e){
				
				List<PlugInterface> pluginInterfaces = PluginManager.loadPlugin();
				algorithmTable.setData(pluginInterfaces);
			}
		});
	}
	
	public void registerTable(AlgorithmTable aTable){
		this.algorithmTable = aTable;
	}
}



class AlgorithmTable extends JTable
{
	private final int columnSize = 3;
	private DefaultTableModel dm = new DefaultTableModel();
	
	public AlgorithmTable(){	
		
		dm.setColumnIdentifiers(new Object[]{ "Algorithm", "Use", "Advance" });

		this.setModel(dm);

		setVisible(true);
	}
	
	
	public void setData(List<PlugInterface> pluginInterfaces){
		
		int row = pluginInterfaces.size();
		Object data[][] = new Object[row][columnSize];
		
		for(int i = 0; i < row; i++){
			
			data[i][0] = new String("Algorithm" + i);
			data[i][1] = new Boolean(false);
			data[i][2] = new String("Config");
		}
		
		//dm.addRow(data);
		dm.setDataVector(data, new Object[]{ "Algorithm", "Use", "Advance" });
		this.setModel(dm);
		
		this.getColumn("Advance").setCellRenderer(new AlgorithmButtonRenderer());
		this.getColumn("Advance").setCellEditor(new ButtonEditor(new JCheckBox()));
		this.getColumn("Advance").setPreferredWidth(80);

		this.getColumn("Use").setCellRenderer(this.getDefaultRenderer(Boolean.class));
		this.getColumn("Use").setCellEditor(this.getDefaultEditor(Boolean.class));
		this.getColumn("Use").setPreferredWidth(40);
		
		this.getColumn("Algorithm").setPreferredWidth(80);
	}

}

class AlgorithmButtonRenderer extends JButton implements TableCellRenderer {

	public AlgorithmButtonRenderer() {
		setOpaque(true);
	}

	public Component getTableCellRendererComponent(JTable table, Object value,
			boolean isSelected, boolean hasFocus, int row, int column) {

		setText((value == null) ? "" : value.toString());
		return this;
	}
}

/**
 * @version 1.0 11/09/98
 */
class ButtonEditor extends DefaultCellEditor {

	protected JButton button;
	private String label;
	private boolean isPushed;

	public ButtonEditor(JCheckBox checkBox) {
		super(checkBox);
		
		button = new JButton();
		button.setOpaque(true);
		button.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				fireEditingStopped();
			}
		});
	}

	public Component getTableCellEditorComponent(JTable table, Object value,
			boolean isSelected, int row, int column) {
		
		label = (value == null) ? "" : value.toString();
		button.setText(label);
		isPushed = true;
		return button;
	}

	public Object getCellEditorValue() {
		if (isPushed) {
			System.out.println("clicked");
		}
		isPushed = false;
		return new String(label);
	}

	public boolean stopCellEditing() {
		isPushed = false;
		return super.stopCellEditing();
	}

	protected void fireEditingStopped() {
		super.fireEditingStopped();
	}
}