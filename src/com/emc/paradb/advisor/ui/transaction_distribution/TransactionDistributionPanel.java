package com.emc.paradb.advisor.ui.transaction_distribution;

import java.awt.BorderLayout;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.ImageIcon;
import javax.swing.JLabel;
import javax.swing.JPanel;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.data.general.DefaultPieDataset;



public class TransactionDistributionPanel extends JPanel
{
	private JLabel distNondistChart = null;
	private JLabel tranDistChart = null;
	private Box box = Box.createHorizontalBox();
	
	public TransactionDistributionPanel(){
		this.setLayout(new BorderLayout());
		this.add(box, BorderLayout.CENTER);
		this.setBorder(BorderFactory.createEtchedBorder());
		
		List<Integer> data = new ArrayList<Integer>();
		HashMap<Integer, Integer> nodeAccess = new HashMap<Integer, Integer>();
		
		for(int i = 0; i < 10; i++){
			data.add(10);
			nodeAccess.put(i, 100);
		}

		setChart(data, nodeAccess);
	}
	
	public void setChart(List<Integer> data, Map<Integer, Integer> nodeAccess){
		
		distNondistChart = DistNonDistChart.createChart(100, 200);
		tranDistChart = DistChart.createChart(nodeAccess);
		box.add(Box.createHorizontalGlue());
		box.add(distNondistChart);
		box.add(tranDistChart);
		box.add(Box.createHorizontalGlue());
	}
	
	
}


class DistNonDistChart {
	
	public static JLabel createChart(int nonDist, int dist){
		
		DefaultPieDataset ds = new DefaultPieDataset();
		ds.setValue(String.format("Non-distributed %d", nonDist), nonDist);
		ds.setValue(String.format("Distributed %d", dist), dist);
		JFreeChart chart = ChartFactory.createPieChart("Dist-Nondist Trans.", ds, true, true, false);
		
		JLabel lb = new JLabel();
		lb.setIcon(new ImageIcon(chart.createBufferedImage(220,200)));
		
		return lb;
	}

}

class DistChart {

	public static JLabel createChart(Map<Integer, Integer> nodeAccess) {
		
		DefaultPieDataset ds = new DefaultPieDataset();
		Set<Integer> ks = nodeAccess.keySet();
		for (int k : ks) {
			if (k != 1) {
				int a = nodeAccess.get(k);
				ds.setValue(String.format("%d %d", k, a), a);
			}
		}
		JFreeChart chart = ChartFactory.createPieChart("Tran Dist",
				ds, false, true, false);

		JLabel lb = new JLabel();
		lb.setIcon(new ImageIcon(chart.createBufferedImage(220, 200)));

		return lb;
	}

}
