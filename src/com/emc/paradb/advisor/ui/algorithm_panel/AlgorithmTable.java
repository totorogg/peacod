package com.emc.paradb.advisor.ui.algorithm_panel;

import java.awt.Color;
import java.awt.Component;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.List;

import javax.swing.DefaultCellEditor;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JTable;
import javax.swing.ListSelectionModel;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.TableModelEvent;
import javax.swing.event.TableModelListener;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableCellRenderer;
import javax.swing.table.TableModel;

import com.emc.paradb.advisor.controller.Controller;
import com.emc.paradb.advisor.plugin.Plugin;

class AlgorithmTable extends JTable
{
	private final int columnSize = 3;
	private int lastSelectedRow = -1;
	private DefaultTableModel dm = new DefaultTableModel();
	private AlgorithmDescriptionPanel adPanel = null;
	
	public AlgorithmTable(){	
		
		dm.setColumnIdentifiers(new Object[]{ "Algorithm", "Use", "Advance" });
		this.setModel(dm);
		setVisible(true);
	}
	
	
	public void setData(List<Plugin> pluginInterfaces)
	{
		int row = pluginInterfaces.size();
		Object data[][] = new Object[row][columnSize];
		
		for(int i = 0; i < row; i++)
		{		
			String name = pluginInterfaces.get(i).getID();
			name = name.substring(name.lastIndexOf("."));
			data[i][0] = new String(name);
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
	
	public void registerDescriptionPanel(AlgorithmDescriptionPanel adPanel)
	{
		this.adPanel = adPanel;
	}
	
	@Override
	//monitor the check box, adjust the algorithms in the algorithm Factory
	public void tableChanged(TableModelEvent e) 
	{
		super.tableChanged(e);
		
		// TODO Auto-generated method stub
		int row = e.getFirstRow();
		int column = e.getColumn();
		if(row < 0 || column != 1)
			return;
		TableModel model = (TableModel)e.getSource();
		Boolean checkBox = (Boolean)model.getValueAt(row, column);
			
		Controller.updateSelectedAlgorithm(row, checkBox.booleanValue());
	}
	
	//if a different is selected, the corresponding description should also be changed
	public void valueChanged(ListSelectionEvent e)
	{
		
		super.valueChanged(e);
		
		if(e.getValueIsAdjusting())
			return;
		
		ListSelectionModel rowSM = (ListSelectionModel)e.getSource();
		int selectedIndex = rowSM.getMinSelectionIndex();
		if(selectedIndex != lastSelectedRow)
		{
			lastSelectedRow = selectedIndex;
			Plugin aPlugin= Controller.getAlgorithm(selectedIndex);
			adPanel.setText(aPlugin.getDescription());
		}
	}
}

class AlgorithmButtonRenderer extends JButton implements TableCellRenderer {

	public AlgorithmButtonRenderer() 
	{
		setOpaque(true);
	}

	public Component getTableCellRendererComponent(JTable table, Object value,
			boolean isSelected, boolean hasFocus, int row, int column) 
	{
		setText((value == null) ? "" : value.toString());
		return this;
	}
}


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