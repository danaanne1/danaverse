package com.ddougher.market;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import java.util.prefs.Preferences;

import javax.swing.AbstractAction;
import javax.swing.JDesktopPane;
import javax.swing.JFrame;
import javax.swing.JInternalFrame;
import javax.swing.JMenu;
import javax.swing.JMenuBar;

import com.ddougher.proxamic.MemoryMappedDocumentStore;

public class Application {
	
	View view = new View();
	Controller controller = new Controller();
	MemoryMappedDocumentStore docStore;
	Preferences rootPrefs = Preferences.userNodeForPackage(getClass());
	
	static class Constants {
		static final String DOC_STORE_BASE_PATH_KEY = "basePath";
		static final String DOC_STORE_DEFAULT_FOLDER_NAME = System.getProperty("user.home") + File.separator + String.join(File.separator, "Documents", "DanaTrade", "Data");
	}
	
	/** Where all the actions go */
	class Controller {
		


		void launchApplication() {
			loadDocumentStore();
			view.mainFrame().setVisible(true);
		}

		
		void launchBackgroundProcess() {
			
		}
		
		
		void exitApplication() {
			saveDocumentStore();
			System.exit(0);
		}

		private void loadDocumentStore() {
			Preferences dsp = rootPrefs.node("DocStore");
			File path = new File(dsp.get(Constants.DOC_STORE_BASE_PATH_KEY, Constants.DOC_STORE_DEFAULT_FOLDER_NAME));
			path.mkdirs();

			File docStoreFile = new File(path,"Database.dt1");
			if (docStoreFile.exists()) {
				try (   FileInputStream fin = new FileInputStream(docStoreFile); 
						BufferedInputStream bin = new BufferedInputStream(fin);
						ObjectInputStream oin = new ObjectInputStream(bin)) {
					docStore = (MemoryMappedDocumentStore)oin.readObject();
				} catch (Exception e) {
					docStore = null;
				}
			} 

			if (docStore == null) {
				docStore = 
					new MemoryMappedDocumentStore
					(
							Optional.of(dsp.get(Constants.DOC_STORE_BASE_PATH_KEY, Constants.DOC_STORE_DEFAULT_FOLDER_NAME)),	
							Optional.empty(),
							Optional.empty(), 
							Optional.empty(), 
							Optional.empty()
					);
			}
		}

		private void saveDocumentStore() {
			Preferences dsp = rootPrefs.node("DocStore");
			File path = new File(dsp.get(Constants.DOC_STORE_BASE_PATH_KEY, Constants.DOC_STORE_DEFAULT_FOLDER_NAME));
			path.mkdirs();
			File docStoreFile = new File(path,"Database.dt1");
			try (	FileOutputStream fout = new FileOutputStream(docStoreFile); 
					BufferedOutputStream bout = new BufferedOutputStream(fout);
					ObjectOutputStream oout = new ObjectOutputStream(bout) ) {
				oout.writeObject(docStore);
				oout.flush();
			} catch (Exception e) {
				// show an error dialog
			}
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
			tools.add(new AbstractAction("Launch Background Process") {
				@Override
				public void actionPerformed(ActionEvent e) {
					controller.launchBackgroundProcess();
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
