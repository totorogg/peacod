package ui.mainframe;

import java.awt.FlowLayout;
import java.awt.GridLayout;

import javax.swing.ButtonGroup;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JTabbedPane;

public class BenchmarkSelectPanel extends JTabbedPane
{
	private JPanel benchmarkSelectPanel = new JPanel();
	
	private JRadioButton bm1 = new JRadioButton("TPC-C");
	private JRadioButton bm2 = new JRadioButton("TPC-H");
	private JRadioButton bm3 = new JRadioButton("TPC-W");
	private JRadioButton bm4 = new JRadioButton("TATP");
	private ButtonGroup bmg = new ButtonGroup();
	
	public BenchmarkSelectPanel(){
		
		benchmarkSelectPanel.setLayout(new GridLayout(10, 2, 10, 10));
		
		benchmarkSelectPanel.add(new JLabel(""));
		benchmarkSelectPanel.add(bm1);
		benchmarkSelectPanel.add(bm2);
		benchmarkSelectPanel.add(bm3);
		benchmarkSelectPanel.add(bm4);
		benchmarkSelectPanel.add(new JLabel(""));

		benchmarkSelectPanel.add(new JButton("Start"));
		benchmarkSelectPanel.add(new JButton("Stop"));
		
		bmg.add(bm1);
		bmg.add(bm2);
		bmg.add(bm3);
		bmg.add(bm4);
		
		this.add(benchmarkSelectPanel, "Benchmarks");
	}
	
}