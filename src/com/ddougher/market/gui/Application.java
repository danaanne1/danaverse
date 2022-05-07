package com.ddougher.market.gui;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.lang.reflect.InvocationTargetException;

import javax.swing.AbstractAction;
import javax.swing.JDesktopPane;
import javax.swing.JFrame;
import javax.swing.JInternalFrame;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JRootPane;

public class Application {
	
	View view = new View();
	Controller controller = new Controller();
	
	/** Where all the actions go */
	class Controller {
		
		void launchApplication() {
			view.mainFrame().setVisible(true);
		}
		
		void exitApplication() {
			System.exit(0);
		}
	}
	
	
	/** where all the ui is created and tracked **/
	@SuppressWarnings("serial")
	class View {

		JDesktopPane desktopPane;
		
		public JFrame mainFrame() {
			JFrame.setDefaultLookAndFeelDecorated(true);		
			JFrame frame = new JFrame("Dana Trade 1.0");
			frame.setDefaultCloseOperation(JFrame.DO_NOTHING_ON_CLOSE);
			frame.setPreferredSize(new Dimension(1500,500));
			frame.addWindowListener(new WindowAdapter() {
				@Override 
				public void windowClosing(WindowEvent e) { 
					System.out.println("Frame Closing"); 
					controller.exitApplication();
				}
				@Override public void windowClosed(WindowEvent e) { System.out.println("Frame closed"); }
				@Override public void windowActivated(WindowEvent e) { System.out.println("Frame activated"); }
				@Override public void windowDeactivated(WindowEvent e) { System.out.println("Frame deactivated"); }
			});
			frame.getRootPane().getContentPane().add(BorderLayout.CENTER, desktopPane());
			frame.getRootPane().setJMenuBar(mainMenuBar());
			frame.pack();
			return frame;
		}
		
		private JDesktopPane desktopPane() {
			desktopPane = new JDesktopPane();
			return desktopPane;
		}
		
		private JMenuBar mainMenuBar() {
			JMenuBar menuBar = new JMenuBar();
			menuBar.add(toolsMenu());
			return menuBar;
		}
		
		private JMenu toolsMenu() {
			
			JMenu tools = new JMenu("Tools", false);
			tools.add(new AbstractAction("Chart") {
				@Override
				public void actionPerformed(ActionEvent e) {
					desktopPane.add(chartFrame(),0);
				}
			});
			return tools;
		}

		private JInternalFrame chartFrame() {
			JInternalFrame iFrame = new JInternalFrame("Charts", true, true, true, false);
			iFrame.setPreferredSize(new Dimension(200,200));
			iFrame.setDefaultCloseOperation(JInternalFrame.DISPOSE_ON_CLOSE);
			iFrame.pack();
			iFrame.setVisible(true);
			return iFrame;
		}
		
	}
	
	
	public static void main(String [] args) throws InvocationTargetException, InterruptedException {
		Application a = new Application();
		a.controller.launchApplication();
	}	

	
}
