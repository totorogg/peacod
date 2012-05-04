package com.emc.paradb.advisor.ui.algorithm_panel;

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
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableCellRenderer;
import javax.swing.table.TableModel;

import com.emc.paradb.advisor.controller.AlgorithmController;
import com.emc.paradb.advisor.controller.DisplayController;
import com.emc.paradb.advisor.plugin.Plugin;

/**
 * algorithm table, for displaying algorithm list
 * @author xpan
 *
 */
class AlgorithmTable extends JTable
{
	private final int columnSize = 3;
	private int lastSelectedRow = -1;
	private DefaultTableModel dm = new DefaultTableModel();
	private AlgorithmDescriptionPanel adPanel = null;
	
	public AlgorithmTable()
	{		
		//dm.setColumnIdentifiers(new Object[]{ "Scheme", "Y/N", "Setting" });
		dm.setColumnIdentifiers(new Object[]{ "Scheme", " " });
		this.setModel(dm);
		setVisible(true);
	}
	
	/**
	 * set algorithm info into the table
	 * @param pluginInterfaces
	 */
	public void setData(List<Plugin> pluginInterfaces)
	{
		int row = pluginInterfaces.size();
		Object data[][] = new Object[row][columnSize];
		
		//add algorithms row by row
		for(int i = 0; i < row; i++)
		{		
			String name = pluginInterfaces.get(i).getID();
			name = name.substring(name.lastIndexOf(".") + 1);
			data[i][0] = new String(name);
			data[i][1] = new Boolean(false);
			data[i][2] = new String("setting");
		}
		
		//dm.addRow(data);
		//dm.setDataVector(data, new Object[]{ "Scheme", "Y/N", "Setting" });
		dm.setDataVector(data, new Object[]{ "Scheme", " " });
		this.setModel(dm);
		this.setRowHeight(30);
		
		//this.getColumn("Setting").setCellRenderer(new AlgorithmButtonRenderer());
		//this.getColumn("Setting").setCellEditor(new ButtonEditor(new JCheckBox()));
		//this.getColumn("Setting").setPreferredWidth(80);
		
		this.getColumn(" ").setCellRenderer(this.getDefaultRenderer(Boolean.class));
		this.getColumn(" ").setCellEditor(this.getDefaultEditor(Boolean.class));
		this.getColumn(" ").setPreferredWidth(40);
		
		this.getColumn("Scheme").setPreferredWidth(80);
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
		
		if(row < 0)
			return;
		
		if(column == 2)
		{
			AlgorithmController.updateSetting(row);
			return;
		}	
		else if(column == 1)
		{
			TableModel model = (TableModel)e.getSource();
			Boolean checkBox = (Boolean)model.getValueAt(row, column);
				
			AlgorithmController.updateSelectedAlgorithm(row, checkBox.booleanValue());
		}
	}
	
	//if a different is selected, the corresponding description should also be changed
	public void valueChanged(ListSelectionEvent e)
	{
		super.valueChanged(e);
		
		if(e.getValueIsAdjusting())
			return;
		
		ListSelectionModel rowSM = (ListSelectionModel)e.getSource();
		int selectedIndex = rowSM.getMinSelectionIndex();
		if(selectedIndex != lastSelectedRow && selectedIndex >= 0)
		{
			lastSelectedRow = selectedIndex;
			Plugin aPlugin= AlgorithmController.getAlgorithm(selectedIndex);
			
			adPanel.setText(aPlugin.getDescription());
			DisplayController.display(selectedIndex);
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

/**
 * customize the button editor
 * so that it can be used in JTable
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