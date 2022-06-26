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
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.prefs.Preferences;

import javax.net.ssl.HttpsURLConnection;
import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.JComboBox;
import javax.swing.JDesktopPane;
import javax.swing.JFrame;
import javax.swing.JInternalFrame;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.event.InternalFrameAdapter;
import javax.swing.event.InternalFrameEvent;

import com.ddougher.market.data.Equity;
import com.ddougher.market.data.Equity.Day;
import com.ddougher.market.data.Equity.Metric;
import com.ddougher.market.data.Equity.Year;
import com.ddougher.market.data.MetricConstants;
import com.ddougher.market.data.Stocks;
import com.ddougher.market.gui.JChart;
import com.ddougher.proxamic.MemoryMappedDocumentStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.theunknowablebits.proxamic.DocumentStore;

public class Application {
	
	View view = new View();
	Controller controller = new Controller();
	MemoryMappedDocumentStore docStore;
	Preferences rootPrefs = Preferences.userNodeForPackage(getClass());
	
	/** Provides linking between named link names and equity or derivative target names */
	ConcurrentMap<String,String> componentLinking = new ConcurrentHashMap<>();
	
	static class Constants {
		static final String TERMINAL_STRING = "__terminal__";
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
			new Thread(()->{
				reportingExceptions(() -> {
					URL url = new URL("https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&sort=ticker&order=asc&limit=1000&apiKey=" + System.getenv("POLYGON_API_KEY"));
					while (url != null) {
						JsonNode root = readFromURL(url);
						docStore.transact(ds->{
							Stocks stocks = ds.get(Stocks.class, "stocks");
							for (JsonNode result: root.get("results")) { 
								saveResultToSymbol(result, ds, stocks);
							}
							ds.put(stocks);
						});
	
						url = null;
						if (root.hasNonNull("next_url")) { 
							url = new URL(root.get("next_url").asText()+"&apiKey="+System.getenv("POLYGON_API_KEY"));
						}
						System.out.println("Backfill Sleeping on API");
					}
				});
			}).start();
		}
		
		void getPreviousDay() {
			LinkedBlockingQueue<String> results = new LinkedBlockingQueue<String>();
			AtomicInteger counter = new AtomicInteger();
			Thread t;
			(t=new Thread(()-> {
				Arrays
				.stream( 
					docStore
						.get(Stocks.class, "stocks")
						.tickers()
						.keySet()
						.stream()
						.sorted()
						.toArray(i->new String[i])
				)
				.parallel()
				.forEach(ticker -> 
					retryingExceptions(4, false, () -> {
						JsonNode root = readFromURL(new URL(
								String.format(
										"https://api.polygon.io/v2/aggs/ticker/%1$s/range/1/minute/%2$s/%2$s?adjusted=true&sort=asc&limit=1000&apiKey=%3$s",
										ticker,
										new SimpleDateFormat("yyyy-MM-dd").format(MetricConstants.previousTradingDate()),
										System.getenv("POLYGON_API_KEY")
								)
						));
						AtomicInteger rcount = new AtomicInteger(0);
						docStore.transact(ds->{
							if (root.has("results")) {
								for (JsonNode result: root.get("results")) { 
									saveAggsToSymbol(result, ds, ticker);
									rcount.getAndIncrement();
								}
							}
						});
						results.put(counter.incrementAndGet() + ": Loaded " + rcount.get() + " results for " + ticker);
					})
				);
				results.add(Constants.TERMINAL_STRING);
			})).start();
			new Thread(() -> {
				reportingExceptions(() -> {
					String s;
					while (t.isAlive()) {
						while (null != (s = results.poll(1, TimeUnit.SECONDS))) {
							if (s==Constants.TERMINAL_STRING) {
								System.out.println("Finished");
								return;
							}
							System.out.println(s);
						}
					}
				});
			}).start();
		}
		
		private void saveAggsToSymbol(JsonNode result, DocumentStore ds, String ticker) {

			Date date = new Date(result.get("t").asLong());
			String tradingYear = Integer.toString(MetricConstants.tradingYearFromDate(date));
			String tradingDay = Integer.toString(MetricConstants.tradingDayFromDate(date));
			int iTradingMinute = MetricConstants.tradingMinuteFromDate(date);
			if (iTradingMinute > 960) 
				throw new IllegalArgumentException("Trading minute too big");
			
			Equity e = ds.get(Equity.class, "Equity/"+ticker);

			// metrics are indirect, so equity only needs to be saved if a new metric is added
			Metric ohlc = e.metrics().get("ohlc");
			if (ohlc == null)
				e.metrics().put("ohlc", ohlc = ds.newInstance(Metric.class));
				ds.put(e);

			// years are indirect, so ohlc only needs to be saved if a new year is added	
			Year year = ohlc.years().get(tradingYear);
			if (year == null)
				ohlc.years().put(tradingYear, year = ds.newInstance(Year.class));
				ds.put(ohlc);
			
			// from here on out everything is a direct member of year, so year must be saved when done
			Day day = year.days().get(tradingDay);
			if (day == null) 
				year.days().put(tradingDay, day = ds.newInstance(Day.class));
			
			
			List<Float []> minutes = day.minutes();
			while (minutes.size() <= iTradingMinute) {
				minutes.add(new Float[] { 0f, 0f, 0f, 0f, 0f, 0f, 0f  } );
			}
			minutes.set(iTradingMinute, new Float [] {
				Double.valueOf(result.get("o").asDouble()).floatValue(),
				Double.valueOf(result.get("h").asDouble()).floatValue(),
				Double.valueOf(result.get("l").asDouble()).floatValue(),
				Double.valueOf(result.get("c").asDouble()).floatValue(),
				Double.valueOf(result.get("v").asDouble()).floatValue(),
				Double.valueOf(result.get("vw").asDouble()).floatValue(),
				Double.valueOf(result.get("n").asDouble()).floatValue(),
			});

			ds.put(year);
		}
		

		private void saveResultToSymbol(JsonNode result, DocumentStore ds, Stocks stocks) {
			System.out.println(result.toString());
			String ticker = result.get("ticker").asText();
			Map<String,Equity> tickers = stocks.tickers();
			Equity e = null;
			e = tickers.get(ticker);
			if (e==null) 
				tickers.put(ticker, e = ds.newInstance(Equity.class, "Equity/"+ticker));
			e
				.withSymbol(ticker)
				.withName(result.get("name").asText())
				.withExchange(result.has("primary_exchange")?result.get("primary_exchange").asText():"")
				.withLocale(result.has("locale")?result.get("locale").asText():"")
				.withType(result.has("type")?result.get("type").asText():"")
				.withActive(result.get("active").asBoolean());

			ds.put(e);
		}
		
		private JsonNode readFromURL(URL url) throws IOException {
			HttpsURLConnection con = (HttpsURLConnection) url.openConnection();
			con.setRequestMethod("GET");
			con.setRequestProperty("Accept","application/json");
			if (con.getResponseCode()!=200) {
				JOptionPane.showMessageDialog(view.desktopPane, con.getResponseMessage().toString(), null, JOptionPane.ERROR_MESSAGE);
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
				e.printStackTrace(System.err);
				// JOptionPane.showOptionDialog(null, e.getMessage(), e.getClass().getName(), JOptionPane.ERROR_MESSAGE, JOptionPane.DEFAULT_OPTION, null, null, null);
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
			tools.add(Utils.actionFu("Chart", ()-> desktopPane.add(chartFrame(),0))); 
			tools.add(Utils.actionFu("Get All Stock Tickers", () -> controller.backfillTickers() ));
			tools.add(Utils.actionFu("Get Previous Day", () -> controller.getPreviousDay() ));
			return tools;
		}

		
		private JInternalFrame chartFrame() {
			JInternalFrame iFrame = new JInternalFrame("Charts", true, true, true, false);
			iFrame.setPreferredSize(new Dimension(200,200));
			iFrame.setDefaultCloseOperation(JInternalFrame.DISPOSE_ON_CLOSE);
			JChart chart = new JChart();
			chart.setBackground(Color.white);
			iFrame.getContentPane().add(chart, BorderLayout.CENTER);
			iFrame.getContentPane().add(chartControl(chart, iFrame), BorderLayout.NORTH);
			iFrame.pack();
			iFrame.setVisible(true);
			return iFrame;
		}
		
		@SuppressWarnings({ "resource" })
		private JPanel chartControl(JChart chart, JInternalFrame iframe) {
			AtomicReference<ChartSequence<Float []>> sequence = new AtomicReference<ChartSequence<Float []>>();
			JPanel p = new JPanel(new GridBagLayout());
			JComboBox<String> combo = 
				new JComboBox<String>(
					docStore.get(Stocks.class, "stocks")
						.tickers()
						.keySet()
						.stream()
						.sorted()
						.toArray(i->new String[i])
				);
			combo.setEditable(false);
			combo.setPrototypeDisplayValue("2022/07/10 TWTR/A C 87.00");
			combo.addItemListener(ie->{
				unloadChartSequence(chart, sequence);
				Calendar c = Calendar.getInstance(TimeZone.getTimeZone("America/New_York"));
				c.set(Calendar.HOUR_OF_DAY, 4);
				c.set(Calendar.MINUTE, 0);
				c.clear(Calendar.SECOND);
				c.clear(Calendar.MILLISECOND);
				ChartSequence<Float []> seq = sequence.get();
				if (seq != null)
					chart.removeChartSequence(sequence.get());
				Equity e = docStore.get(Stocks.class, "stocks").tickers().get(ie.getItem());
				sequence.set(new MinuteChartSequence(e, "ohlc", c.getTime(), 0, true, true));
				chart.addChartSequence(sequence.get(), JChart.CandlePainter, Optional.empty(), Optional.empty(), Optional.empty());
			});
			iframe.addInternalFrameListener(new InternalFrameAdapter() {
				@Override public void internalFrameClosing(InternalFrameEvent e) { unloadChartSequence(chart, sequence); }
			});
			p.add(combo, new GridBagConstraints(0, 0, 1, 1, 0, 1, GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(0,0,0,0), 0, 0));
			p.add(new JPanel(),  new GridBagConstraints(GridBagConstraints.RELATIVE, 0, GridBagConstraints.REMAINDER, 1, 1, 1, GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(0,0,0,0), 0, 0));
			return p;
		}

		private void unloadChartSequence(JChart chart, AtomicReference<ChartSequence<Float[]>> sequence) {
			ChartSequence<Float []> seq = sequence.get();
			if (seq!=null) {
				((MinuteChartSequence)seq).close();
				chart.removeChartSequence(seq);
				sequence.set(null);
			}
		}
		
	}
	
	// Simplified Exception handling
	interface ESupplier<T> {
		T get() throws Exception;
	}
	interface ERunnable {
		void run() throws Exception;
	}
	<T> T reportingExceptions(boolean displayAlert, ESupplier<T> s) {
		return retryingExceptions(1, displayAlert, s);
	}
	<T> T reportingExceptions(ESupplier<T> s) {
		return reportingExceptions(false, s);
	}
	void reportingExceptions(boolean displayAlert, ERunnable r) {
		reportingExceptions(displayAlert, () -> { r.run(); return null; });
	}
	void reportingExceptions(ERunnable r) {
		reportingExceptions(false, r);
	}
	<T> T retryingExceptions(int attempts, boolean displayAlert, ESupplier<T> s) {
		while(attempts > 0) {
			try {
				return s.get();
			} catch (Exception e) {
				if (attempts > 1) {
					System.out.println("retrying " + attempts + " more times for " + e);
					continue;
				}
				e.printStackTrace(System.err);
				if (displayAlert) JOptionPane.showMessageDialog(view.desktopPane, e.getMessage(), e.getClass().getName(), JOptionPane.ERROR_MESSAGE);
			}
			attempts--;
		}
		return null;
	}
	<T> T retryingExceptions(int attempts, ESupplier<T> s) {
		return retryingExceptions(attempts, false, s);
	}
	void retryingExceptions(int attempts, boolean displayAlert, ERunnable r) {
		retryingExceptions(attempts, displayAlert, () -> { r.run(); return null; });
	}
	void retryingExceptions(int attempts, ERunnable r) {
		retryingExceptions(attempts, false, r);
	}

	public static void main(String [] args) throws InvocationTargetException, InterruptedException {
		Application a = new Application();
		a.controller.launchApplication();
	}	

	
}
