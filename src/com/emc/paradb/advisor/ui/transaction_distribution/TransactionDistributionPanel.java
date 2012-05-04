package com.emc.paradb.advisor.ui.transaction_distribution;

import java.awt.Color;
import java.awt.Font;
import java.awt.Paint;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.ImageIcon;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextPane;
import javax.swing.text.MutableAttributeSet;
import javax.swing.text.SimpleAttributeSet;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.labels.ItemLabelAnchor;
import org.jfree.chart.labels.ItemLabelPosition;
import org.jfree.chart.labels.StandardPieSectionLabelGenerator;
import org.jfree.chart.plot.PiePlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.category.BarRenderer;
import org.jfree.chart.renderer.category.CategoryItemRenderer;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.general.DefaultPieDataset;
import org.jfree.data.general.PieDataset;
import org.jfree.ui.TextAnchor;
import org.jfree.util.Rotation;

import com.emc.paradb.advisor.controller.DisplayController;
import com.emc.paradb.advisor.ui.mainframe.MainFrame;

/**
 *This metric measures the total number of distributed transactions of the workload resulted from the partitioning scheme(s). 
 *Since distributed transactions dominate the total workload execution cost, the fewer number of them implies the better performance.
 *For a single scheme, the left figure shows the number of distributed transactions. The right figure shows the distribution of the number of
 *data nodes that a transaction spanned over."
 * @author Xin Pan
 *
 */
public class TransactionDistributionPanel extends JPanel
{
	private Box box = Box.createHorizontalBox();
	private JLabel transactionChart = null;
	
	public TransactionDistributionPanel()
	{
		this.setLayout(new BoxLayout(this, BoxLayout.Y_AXIS));
		
		JTextPane description = new JTextPane();
		MutableAttributeSet set = new SimpleAttributeSet();
		//StyleConstants.setLineSpacing(set, -0.4f);
		//StyleConstants.setFontFamily(set, "Comic Sans MS");
		//description.setParagraphAttributes(set, true);
		
		description.setText("DESCRIPTION:\n  This metric measures the total number of distributed transactions of the workload resulted from the partitioning scheme(s). " 
							+ "Since distributed transactions dominate the total workload execution cost, the fewer number of them implies the better performance." +
							"For a single scheme, the left figure shows the number of distributed transactions. The right figure shows the distribution of the number of " + "" +
							"data nodes that a transaction spanned over.");
		description.setEditable(false);

		Box labelBox = Box.createHorizontalBox();
		//JLabel label = new JLabel("Partition scheme: CountMaxRR");
		//labelBox.add(label);
		labelBox.add(Box.createHorizontalGlue());
		this.add(description);
		this.add(Box.createVerticalStrut(10));
		this.add(labelBox);
		this.add(Box.createVerticalStrut(15));
		this.add(box);
		this.add(Box.createVerticalStrut(5));
				
		this.setBorder(BorderFactory.createEtchedBorder());

		//define its call back object and register it in controller
		DisplayController.registerTransactionDistributionCB(new TransactionDistributionCB()
		{
			@Override
			public void draw(int dist, int nonDist, Map<Integer, Integer> nodeAccess) 
			{
				// TODO Auto-generated method stub
				
				PieDataset result = createDistNonDistDataset(dist, nonDist);
				PieDataset result2 = createNodeAccessDataset(nodeAccess);
				
				JFreeChart chart = createChart(result, "#Distributed vs. #NonDistributed", true);
				//JFreeChart chart2 = createChart(result2, "Nodes Coverage of Distributed Transactions", false);
				JFreeChart chart2 = createChart(result2, "Nodes Coverage of Transactions", false);
				
				chart2.removeLegend();
				
				ChartPanel chartPanel = new ChartPanel(chart);
				ChartPanel chartPanel2 = new ChartPanel(chart2);
				
				box.removeAll();
				box.add(Box.createHorizontalGlue());
				box.add(chartPanel);
				box.add(chartPanel2);
				box.add(Box.createHorizontalGlue());
				box.updateUI();
				box.validate();
			}

			@Override
			public void draw(HashMap<String, Integer> tDMap) {
				// TODO Auto-generated method stub
				box.removeAll();

				transactionChart = TransactionChart.createChart(tDMap);
				box.add(Box.createHorizontalGlue());
				box.add(transactionChart);
				box.add(Box.createHorizontalGlue());
				box.updateUI();
				box.validate();
			}
		});
	}
	//create its chart
	private  PieDataset createDistNonDistDataset(int dist, int nonDist) 
	{
        DefaultPieDataset result = new DefaultPieDataset();
        result.setValue("Distributed", dist);
        result.setValue("Non-Distributed", nonDist);
        return result;
    }
	
	private  PieDataset createNodeAccessDataset(Map<Integer, Integer> nodeAccess) 
	{
        DefaultPieDataset result = new DefaultPieDataset();
		
        for(Integer node : nodeAccess.keySet())
        {
        	result.setValue(String.format("%s", node) , nodeAccess.get(node));
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
	    //chart.getTitle().setFont(new Font("Comic Sans MS", Font.BOLD, 18));
	    chart.getTitle().setFont(MainFrame.mainFont.deriveFont(Font.BOLD, 18));
	    chart.setBorderVisible(false);
	    chart.setBorderPaint(null);

	    PiePlot plot = (PiePlot) chart.getPlot();
	    //plot.setLabelFont(new Font("Comic Sans MS", Font.PLAIN, 12));
	    plot.setLabelFont(MainFrame.mainFont.deriveFont(Font.PLAIN, 12));
	    plot.setSimpleLabels(simple);
	    plot.setOutlineVisible(false);
	    plot.setLabelGenerator(new StandardPieSectionLabelGenerator(
                "{0} {2}",
                NumberFormat.getNumberInstance(),
                new DecimalFormat("0%")));
	    plot.setDirection(Rotation.CLOCKWISE);
	    plot.setBackgroundPaint(Color.white);
	    plot.setInteriorGap(0.06f);
	   
		Color[] colors = {Color.red, Color.blue, Color.green,
                Color.yellow, Color.orange, Color.cyan,
                Color.magenta, Color.blue};
        PieRenderer renderer = new PieRenderer(colors);
        renderer.setColor(plot, dataset);
		
	    return chart;
	}
	
	
}


class TransactionChart 
{
	public static JLabel createChart(HashMap<String, Integer> wDVarMap) 
	{
		DefaultCategoryDataset categoryDataset = new DefaultCategoryDataset();
		String table = "";
		for (String algorithm : wDVarMap.keySet()) {
			int index = algorithm.lastIndexOf(".");
			String name = algorithm.substring(index + 1);
			categoryDataset.setValue(wDVarMap.get(algorithm), "", name);
		}

		//JFreeChart chart = ChartFactory.createBarChart("Distributed Transactions", // Title
		JFreeChart chart = ChartFactory.createBarChart(null, // Title
				"Schemes", // X-Axis label
				"# transactions", // Y-Axis label
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

class PieRenderer
{
    private Color[] color;
   
    public PieRenderer(Color[] color)
    {
        this.color = color;
    }       
   
    public void setColor(PiePlot plot, PieDataset dataset)
    {
        List <Comparable> keys = dataset.getKeys();
        int aInt;
       
        for (int i = 0; i < keys.size(); i++)
        {
            aInt = i % this.color.length;
            plot.setSectionPaint(keys.get(i), this.color[aInt]);
        }
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