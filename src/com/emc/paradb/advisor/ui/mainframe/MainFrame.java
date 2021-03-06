package com.emc.paradb.advisor.ui.mainframe;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Font;
import java.awt.Insets;
import java.io.File;
import java.text.AttributedCharacterIterator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import javax.swing.JFrame;
import javax.swing.JSplitPane;
import javax.swing.UIManager;
import javax.swing.UnsupportedLookAndFeelException;
import javax.swing.plaf.basic.BasicLookAndFeel;

import com.emc.paradb.advisor.controller.PrepareController;
import com.emc.paradb.advisor.ui.algorithm_panel.AlgorithmSelectPanel;
//import com.sun.java.swing.plaf.nimbus.NimbusLookAndFeel;
import com.sun.java.swing.plaf.windows.WindowsLookAndFeel;

/**
 * Entry point of sharding-simulator
 * 
 * @author XP
 *
 */
public class MainFrame extends JFrame {
	public static Font mainFont;
	public static int scWidth = 1200;
	public static int scHeight = 540;

	//middle panel for displaying charts
	private static DisplayPanel displayPanel;
	//left panel for displaying options, like database, benchmark, etc.
	private static BenchmarkSelectPanel benchmarkSelectPanel;
	//right panel for selecting algorithms, selecting displaying statistics
	private static AlgorithmSelectPanel algorithmSelectPanel;
	private static JSplitPane splitPane = new JSplitPane(
			JSplitPane.HORIZONTAL_SPLIT, true);

	public static void main(String[] argv) {
		MainFrame mainFrame = new MainFrame();
		mainFrame.setVisible(true);
	}

	public MainFrame() {
		
		//load xml configuration properties
		PrepareController.loadParameters();
	/*	try {

			UIManager.setLookAndFeel(WindowsLookAndFeel.class.getName());
			// NapkinLookAndFeel,NimbusLookAndFeel,WindowsLookAndFeel
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedLookAndFeelException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
*/
		//some kind of central UI alteration
		try {
			//mainFont = Font.createFont(Font.TRUETYPE_FONT,
			//		new File(System.getProperty("user.dir")
			//				+ "/resources/Frutiger.ttf"));
			//mainFont = mainFont.deriveFont(Font.PLAIN, 12.0f);
			mainFont = new Font("Sans Serif", Font.PLAIN, 18);
		} catch (Exception e) {
			mainFont = new Font("Sans Serif", Font.PLAIN, 18);
			System.out.println("load font error: using system font");
			e.printStackTrace();
		}

		Enumeration keys = UIManager.getDefaults().keys();
		while (keys.hasMoreElements()) {
			Object key = keys.nextElement();
			Object value = UIManager.get(key);
			if (value instanceof javax.swing.plaf.FontUIResource) {
				// UIManager.put (key, new Font( "Comic Sans MS", Font.PLAIN,
				// 12));
				// UIManager.put (key, new Font( "Frutiger", Font.PLAIN, 12));
				//UIManager.put(key, mainFont.deriveFont(Font.PLAIN, 12));
				UIManager.put(key, mainFont.deriveFont(Font.PLAIN, 18));
			}
		}
		// Map<? extends AttributedCharacterIterator.Attribute,?> attributes =
		// new HashMap<? extends AttributedCharacterIterator.Attribute,?>();

		// UIManager.put( "Button.font", new Font( "Verdana", Font.BOLD,
		// ABORT));
		// UIManager.put("Button.font", new Font( "Verdana", Font.BOLD, ABORT));
		UIManager.put("Panel.background", Color.WHITE);

		//init each panels
		displayPanel = new DisplayPanel();
		algorithmSelectPanel = new AlgorithmSelectPanel();
		benchmarkSelectPanel = new BenchmarkSelectPanel();

		//this.setSize(800, 360);
		this.setSize(scWidth, scHeight);
		this.setTitle("PEACOD");
		this.setLayout(new BorderLayout());
		this.setDefaultCloseOperation(EXIT_ON_CLOSE);

		splitPane.setLeftComponent(displayPanel);
		splitPane.setRightComponent(algorithmSelectPanel);
		splitPane.setDividerSize(5);

		this.add(splitPane, BorderLayout.CENTER);
		//splitPane.setDividerLocation(470);scWidth
		splitPane.setDividerLocation(scWidth * 3 / 5);
		this.add(benchmarkSelectPanel, BorderLayout.WEST);
	}
}