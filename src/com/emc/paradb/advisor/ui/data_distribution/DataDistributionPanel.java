package com.emc.paradb.advisor.ui.data_distribution;

import javax.swing.JPanel;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Font;
import java.awt.Paint;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.ImageIcon;
import javax.swing.JLabel;
import javax.swing.JTextArea;
import javax.swing.JTextField;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.labels.ItemLabelAnchor;
import org.jfree.chart.labels.ItemLabelPosition;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.category.BarRenderer;
import org.jfree.chart.renderer.category.CategoryItemRenderer;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.ui.TextAnchor;

import com.emc.paradb.advisor.controller.DisplayController;
import com.emc.paradb.advisor.controller.EvaluateController;

/**
 * DESCRIPTION: \n This metric describes how uniformly data are distributed across data nodes. "
 * "For a single scheme's descriptive representation, each node's total tuples will be showed. For the comparasion representation, "
 * "the metric is represented by the standard variance that is normalized to a value between zero and one. \n");
 * @author xpan
 *
 */
public class DataDistributionPanel extends JPanel {
	private JLabel dataChart = null;
	private Box box = Box.createHorizontalBox();

	public DataDistributionPanel() {
		this.setLayout(new BorderLayout());

		JTextArea description = new JTextArea(
				"DESCRIPTION: \n This metric describes how uniformly data are distributed across data nodes. "
						+ "For a single scheme's descriptive representation, each node's total tuples will be showed. For the comparasion representation, "
						+ "the metric is represented by the standard variance that is normalized to a value between zero and one. \n");

		description.setLineWrap(true);

		this.add(description, BorderLayout.NORTH);
		this.add(box, BorderLayout.CENTER);
		this.setBorder(BorderFactory.createEtchedBorder());

		//define the call back function and register it in controller
		DisplayController.registerDataDistributionCB(new DataDistributionCB() {
			public void draw(List<Long> data) {
				box.removeAll();

				dataChart = DataChart.createChart(data);
				box.add(Box.createHorizontalGlue());
				box.add(dataChart);
				box.add(Box.createHorizontalGlue());
				box.repaint();
				box.validate();
			}

			@Override
			public void draw(HashMap<String, Float> dDVarMap) {
				// TODO Auto-generated method stub
				box.removeAll();

				dataChart = DataChart.createChart(dDVarMap);
				box.add(Box.createHorizontalGlue());
				box.add(dataChart);
				box.add(Box.createHorizontalGlue());
				box.repaint();
				box.validate();
			}
		});
	}
}

class DataChart {
	public static JLabel createChart(List<Long> tuples) {

		DefaultCategoryDataset categoryDataset = new DefaultCategoryDataset();
		String table = "";
		for (int i = 0; i < tuples.size(); i++) {
			//categoryDataset.setValue(tuples.get(i), i + "", String.valueOf(i+1));
			categoryDataset.setValue(tuples.get(i), "", i+"");
		}

		JFreeChart chart = ChartFactory.createBarChart("", // Title
				"Data Nodes", // X-Axis label
				"Data (Tuples)", // Y-Axis label
				categoryDataset, // Dataset,
				PlotOrientation.VERTICAL, false, true, false);

		 final CategoryItemRenderer renderer = new CustomRenderer(
		            new Paint[] {Color.red, Color.blue, Color.green,
		                Color.yellow, Color.orange, Color.cyan,
		                Color.magenta, Color.blue}
		 );
//		        renderer.setLabelGenerator(new StandardCategoryLabelGenerator());
		        renderer.setItemLabelsVisible(true);
		        final ItemLabelPosition p = new ItemLabelPosition(
		            ItemLabelAnchor.CENTER, TextAnchor.CENTER, TextAnchor.CENTER, 45.0
		        );
		        renderer.setPositiveItemLabelPosition(p);
		chart.getCategoryPlot().setRenderer(renderer);
		
		
		JLabel lb = new JLabel();
		lb.setIcon(new ImageIcon(chart.createBufferedImage(600, 320)));

		return lb;
	}

	public static JLabel createChart(HashMap<String, Float> dDVarMap) {

		DefaultCategoryDataset categoryDataset = new DefaultCategoryDataset();
		String table = "";
		for (String algorithm : dDVarMap.keySet()) {
			int index = algorithm.lastIndexOf(".");
			String name = algorithm.substring(index + 1);
			categoryDataset.setValue(dDVarMap.get(algorithm), "", name);
		}

		JFreeChart chart = ChartFactory.createBarChart("", // Title
				"Schemes", // X-Axis label
				"Variance", // Y-Axis label
				categoryDataset, // Dataset,
				PlotOrientation.VERTICAL, false, true, false);

		
		 final CategoryItemRenderer renderer = new CustomRenderer(
		            new Paint[] {Color.red, Color.blue, Color.green,
		                Color.yellow, Color.orange, Color.cyan,
		                Color.magenta, Color.blue}
		 );
//		        renderer.setLabelGenerator(new StandardCategoryLabelGenerator());
		        renderer.setItemLabelsVisible(true);
		        final ItemLabelPosition p = new ItemLabelPosition(
		            ItemLabelAnchor.CENTER, TextAnchor.CENTER, TextAnchor.CENTER, 45.0
		        );
		        renderer.setPositiveItemLabelPosition(p);
		chart.getCategoryPlot().setRenderer(renderer);
		
	
		JLabel lb = new JLabel();
		lb.setIcon(new ImageIcon(chart.createBufferedImage(600, 320)));

		return lb;
	}
	

}

class CustomRenderer extends BarRenderer
{

       /** The colors. */
       private Paint[] colors;

       /**
        * Creates a new renderer.
        *
        * @param colors  the colors.
        */
       public CustomRenderer(final Paint[] colors) {
           this.colors = colors;
       }

       /**
        * Returns the paint for an item.  Overrides the default behaviour inherited from
        * AbstractSeriesRenderer.
        *
        * @param row  the series.
        * @param column  the category.
        *
        * @return The item color.
        */
       public Paint getItemPaint(final int row, final int column) {
           return this.colors[column % this.colors.length];
       }
}
