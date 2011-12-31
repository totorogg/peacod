package com.emc.paradb.advisor.ui.transaction_distribution;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Font;
import java.awt.Insets;
import java.awt.Paint;
import java.awt.font.TextAttribute;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.ImageIcon;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextArea;
import javax.swing.JTextPane;
import javax.swing.text.MutableAttributeSet;
import javax.swing.text.SimpleAttributeSet;
import javax.swing.text.StyleConstants;
import javax.swing.text.StyledDocument;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.labels.PieSectionLabelGenerator;
import org.jfree.chart.labels.StandardPieSectionLabelGenerator;
import org.jfree.chart.plot.AbstractPieLabelDistributor;
import org.jfree.chart.plot.PieLabelDistributor;
import org.jfree.chart.plot.PiePlot;
import org.jfree.chart.plot.PiePlot3D;
import org.jfree.data.general.DefaultPieDataset;
import org.jfree.data.general.PieDataset;
import org.jfree.ui.RectangleInsets;
import org.jfree.util.Rotation;



public class TransactionDistributionPanel extends JPanel
{
	private JLabel tranDistChart = null;
	private Box box = Box.createHorizontalBox();
	
	public TransactionDistributionPanel()
	{
		this.setLayout(new BoxLayout(this, BoxLayout.Y_AXIS));
		
		JTextPane description = new JTextPane();
		MutableAttributeSet set = new SimpleAttributeSet();
		StyleConstants.setLineSpacing(set, -0.4f);
		StyleConstants.setFontFamily(set, "Comic Sans MS");
		description.setParagraphAttributes(set, true);
		
		description.setText("DESCRIPTION:\n" +
				  "Left Pie shows the ratio between distributed and non-distributed Xacts. " +
				  "Right Pie shows the ratio among node number that a distributed transaction can access.");
		description.setEditable(false);

		this.add(description);
		this.add(Box.createVerticalStrut(45));
		this.add(box);
		this.add(Box.createVerticalStrut(45));
		
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
		
		PieDataset result = createDataset1();
		PieDataset result2 = createDataset2();
		
		JFreeChart chart = createChart(result, "Distributed/NonDistributed", true);
		JFreeChart chart2 = createChart(result2, "Transaction Coverage(Node)", false);
		
		
		chart2.removeLegend();
		
		ChartPanel chartPanel = new ChartPanel(chart);
		ChartPanel chartPanel2 = new ChartPanel(chart2);
		
		box.add(Box.createHorizontalGlue());
		box.add(chartPanel);
		box.add(chartPanel2);
		box.add(Box.createHorizontalGlue());
	}
	private  PieDataset createDataset1() 
	{
        DefaultPieDataset result = new DefaultPieDataset();
        result.setValue("Distributed", 60);
        result.setValue("Non-Distributed", 40);
        return result;
    }
	
	private  PieDataset createDataset2() 
	{
        DefaultPieDataset result = new DefaultPieDataset();
		
        for(int i = 1; i < 5; i++){
        	
			result.setValue(String.format("%s", i) , i * 10);
		}
        return result;
    }
	
	private JFreeChart createChart(PieDataset dataset, String title, boolean simple) 
	{
	    
	    JFreeChart chart = ChartFactory.createPieChart(
	        title,  				// chart title
	        dataset,                // data
	        false,                   // include legend
	        true,
	        false
	    );
	    chart.getTitle().setFont(new Font("Comic Sans MS", Font.PLAIN, 18));
	    chart.setBorderVisible(false);
	    chart.setBorderPaint(null);

	    PiePlot plot = (PiePlot) chart.getPlot();
	    plot.setLabelFont(new Font("Comic Sans MS", Font.PLAIN, 12));
	    plot.setSimpleLabels(simple);
	    plot.setOutlineVisible(false);
	    plot.setLabelGenerator(new StandardPieSectionLabelGenerator(
                "{0} {2}",
                NumberFormat.getNumberInstance(),
                new DecimalFormat("0%")));
	    plot.setDirection(Rotation.CLOCKWISE);
	    plot.setBackgroundPaint(Color.white);
	    plot.setInteriorGap(0.06f);
	    return chart;
	}
	
	class MyPieLabelDistributor extends PieLabelDistributor
	{
		public MyPieLabelDistributor(int count)
		{
			super(count);
			this.adjustInwards();
			this.spreadEvenly(0.1, 0.2);
		}
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
