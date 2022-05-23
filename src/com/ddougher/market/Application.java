package com.ddougher.market;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.prefs.Preferences;

import javax.net.ssl.HttpsURLConnection;
import javax.swing.AbstractAction;
import javax.swing.JComboBox;
import javax.swing.JDesktopPane;
import javax.swing.JFrame;
import javax.swing.JInternalFrame;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JOptionPane;
import javax.swing.JPanel;

import com.ddougher.market.data.Equity;
import com.ddougher.market.data.Stocks;
import com.ddougher.market.data.Stocks.Family;
import com.ddougher.market.gui.JChart;
import com.ddougher.proxamic.MemoryMappedDocumentStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Application {
	
	View view = new View();
	Controller controller = new Controller();
	MemoryMappedDocumentStore docStore;
	Preferences rootPrefs = Preferences.userNodeForPackage(getClass());
	
	/** Provides linking between named link names and equity or derivative target names */
	ConcurrentMap<String,String> componentLinking = new ConcurrentHashMap<>();
	
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

		void backfillTickers() {
			reportingExceptionsAsAlerts(() -> {
				URL url = new URL("https://api.polygon.io/v3/reference/tickers?type=CS&market=stocks&active=true&sort=ticker&order=asc&limit=1000&apiKey=" + System.getenv("POLYGON_API_KEY"));
				while (url != null) {
					JsonNode root = readFromURL(url);
					for (JsonNode result: root.get("results")) { 
						saveResultToSymbol(result);
					}

					url = null;
					if (root.hasNonNull("next_url")) { 
						url = new URL(root.get("next_url").asText()+"&apiKey="+System.getenv("POLYGON_API_KEY"));
					}

					Thread.sleep(12000); // 5 api calls a minute
				}
			});
		}

		private void saveResultToSymbol(JsonNode result) {
			docStore.transact((ds)->{
				String ticker = result.get("ticker").asText();
				boolean isActive = result.get("active").asBoolean();
				
				Stocks stocks = ds.get(Stocks.class, "stocks");
				Family family = stocks.family(result.get("type").asText());
				Map<String,Equity> actives = family.active();
				Map<String,Equity> inactives = family.inactive();

				Equity e = null;
				if (isActive) {
					if (inactives.containsKey(ticker)) {
						actives.put(ticker, inactives.get(ticker));
						inactives.remove(ticker);
					}
					e = actives.get(ticker);
					if (e==null) 
						actives.put(ticker, e = ds.newInstance(Equity.class));
				} else {
					if (actives.containsKey(ticker)) {
						inactives.put(ticker, actives.get(ticker));
						actives.remove(ticker);
					}
					e = inactives.get(ticker);
					if (e==null) 
						inactives.put(ticker, e = ds.newInstance(Equity.class));
				}
				
				e
					.withSymbol(ticker)
					.withName(result.get("name").asText())
					.withExchange(result.get("primary_exchange").asText())
					.withLocale(result.get("locale").asText())
					.withType(result.get("type").asText())
					.withActive(result.get("active").asBoolean());

				ds.put(e);
				ds.put(family);
				ds.put(stocks);
			});
		}
		
		private JsonNode readFromURL(URL url) throws IOException {
			HttpsURLConnection con = (HttpsURLConnection) url.openConnection();
			con.setRequestMethod("GET");
			con.setRequestProperty("Accept","application/json");
			if (con.getResponseCode()!=200) {
				JOptionPane.showMessageDialog(view.desktopPane, con.getResponseMessage(), null, JOptionPane.ERROR_MESSAGE);
				con.disconnect();
				return null;
			}
			JsonNode root = null;
			try ( InputStream in = con.getInputStream(); 
					BufferedInputStream bin = new BufferedInputStream(in)) {
				ObjectMapper mapper = new ObjectMapper();
				root = mapper.readTree(bin);
			}
			con.disconnect();
			return root;
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
				JOptionPane.showOptionDialog(null, e.getMessage(), null, JOptionPane.ERROR_MESSAGE, JOptionPane.DEFAULT_OPTION, null, null, null);
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
			tools.add(new AbstractAction("Backfill Tickers") {
				@Override
				public void actionPerformed(ActionEvent e) {
					controller.backfillTickers();
				}
			});
			return tools;
		}

		private JInternalFrame chartFrame() {
			JInternalFrame iFrame = new JInternalFrame("Charts", true, true, true, false);
			iFrame.setPreferredSize(new Dimension(200,200));
			iFrame.setDefaultCloseOperation(JInternalFrame.DISPOSE_ON_CLOSE);
			JChart chart = new JChart();
			chart.setBackground(Color.white);
			iFrame.getContentPane().add(chart, BorderLayout.CENTER);
			iFrame.getContentPane().add(chartControl(chart), BorderLayout.NORTH);
			iFrame.pack();
			iFrame.setVisible(true);
			return iFrame;
		}
		
		private JPanel chartControl(JChart chart) {
			
			JPanel p = new JPanel(new GridBagLayout());
			JComboBox<String> combo = new JComboBox<String>();
			combo.setEditable(true);
			combo.setPrototypeDisplayValue("2022/07/10 TWTR/A C 87.00");
			p.add(combo, new GridBagConstraints(0, 0, 1, 1, 0, 1, GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(0,0,0,0), 0, 0));
			p.add(new JPanel(),  new GridBagConstraints(GridBagConstraints.RELATIVE, 0, GridBagConstraints.REMAINDER, 1, 1, 1, GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(0,0,0,0), 0, 0));
			return p;
		}
		
	}
	
	// Simplified Exception handling
	interface ESupplier<T> {
		T get() throws Exception;
	}
	<T> T reportingExceptionsAsAlerts(ESupplier<T> s) {
		try {
			return s.get();
		} catch (Exception e) {
			JOptionPane.showMessageDialog(view.desktopPane, e.getMessage(), null, JOptionPane.ERROR_MESSAGE);
			return null;
		}
	}
	interface ERunnable {
		void run() throws Exception;
	}
	void reportingExceptionsAsAlerts(ERunnable r) {
		reportingExceptionsAsAlerts(() -> { r.run(); return null; });
	}

	public static void main(String [] args) throws InvocationTargetException, InterruptedException {
		Application a = new Application();
		a.controller.launchApplication();
	}	

	
}
