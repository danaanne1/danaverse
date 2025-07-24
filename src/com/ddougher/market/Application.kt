package com.ddougher.market

import com.ddougher.market.application.CSVImporter
import com.ddougher.market.application.StockDataBrowser
import com.ddougher.market.data.core.Stocks
import com.ddougher.market.polygon.BackfilTickers
import com.ddougher.proxamic.DocumentStore
import com.ddougher.proxamic.MemoryMappedDocumentStore
import com.ddougher.remotes.RemoteDocumentStoreClient
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import java.awt.BorderLayout
import java.awt.Dimension
import java.awt.event.ActionEvent
import java.awt.event.WindowAdapter
import java.awt.event.WindowEvent
import javax.swing.AbstractAction
import javax.swing.Action
import javax.swing.JOptionPane
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.File
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.net.InetSocketAddress
import java.util.*
import java.util.prefs.Preferences
import javax.swing.*

@OptIn(DelicateCoroutinesApi::class)
class Application  {

    val preferences = Preferences.userNodeForPackage(javaClass).apply {
        node(Constants.DOC_STORE_NODE).apply { 
            put(Constants.DOC_STORE_BASE_PATH_KEY, get(Constants.DOC_STORE_BASE_PATH_KEY, Constants.DOC_STORE_DEFAULT_FOLDER_NAME))
            put(Constants.REMOTE_STORE_DIRECTORY_KEY, get(Constants.REMOTE_STORE_DIRECTORY_KEY, Constants.DOC_STORE_DEFAULT_FOLDER_NAME + File.separator + "remote"))
        }
        node(Constants.REMOTE_STORE_NODE).apply { 
            put(Constants.REMOTE_STORE_HOST_KEY, get(Constants.REMOTE_STORE_HOST_KEY, "localhost"))
            putInt(Constants.REMOTE_STORE_PORT_KEY, getInt(Constants.REMOTE_STORE_PORT_KEY, 3262))
            putBoolean(Constants.REMOTE_STORE_ENABLED_KEY, getBoolean(Constants.REMOTE_STORE_ENABLED_KEY, false))
        }
        node("Polygon").apply { put("apiKey", get("apiKey", "unknown")) }
    }


    val docStore: MemoryMappedDocumentStore =
            preferences.node(Constants.DOC_STORE_NODE).get(Constants.DOC_STORE_BASE_PATH_KEY, Constants.DOC_STORE_DEFAULT_FOLDER_NAME).let { path ->
                @Suppress("ComplexRedundantLet")
                File(path).apply { mkdirs() }.let { File(it, "Database.dt1") }.let { file ->
                    if (file.exists())
                        ObjectInputStream(BufferedInputStream(file.inputStream(), 65536)).use { it.readObject() as MemoryMappedDocumentStore }
                    else
                        MemoryMappedDocumentStore(Optional.of(path), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty())
                }
            }
            
    // Remote document store (initialized if enabled in preferences)
    val remoteStore: DocumentStore? = if (preferences.node(Constants.REMOTE_STORE_NODE).getBoolean(Constants.REMOTE_STORE_ENABLED_KEY, false)) {
        // Get remote store configuration from preferences
        val remoteServerHost = preferences.node(Constants.REMOTE_STORE_NODE).get(Constants.REMOTE_STORE_HOST_KEY, "localhost")
        val remoteServerPort = preferences.node(Constants.REMOTE_STORE_NODE).getInt(Constants.REMOTE_STORE_PORT_KEY, 3262)
        val remoteStoreDirectory = preferences.node(Constants.DOC_STORE_NODE).get(Constants.REMOTE_STORE_DIRECTORY_KEY, 
            Constants.DOC_STORE_DEFAULT_FOLDER_NAME + File.separator + "remote")
        
        // Create the remote document store client
        val serverAddress = InetSocketAddress(remoteServerHost, remoteServerPort)
        RemoteDocumentStoreClient(serverAddress, remoteStoreDirectory)
    } else {
        null
    }
    
    // Currently selected document store (defaults to local store)
    var selectedDocStore: DocumentStore = docStore

    inner class View {

        val desktopPane = JDesktopPane()
        
        // Status bar for displaying connection status
        val statusBar = JPanel(BorderLayout()).apply {
            border = BorderFactory.createEtchedBorder()
            add(JLabel("Local document store: Connected"), BorderLayout.WEST)
            
            // Remote connection status label
            val remoteStatusLabel = JLabel().apply {
                text = if (remoteStore != null) 
                    "Remote document store: Connected" 
                else 
                    "Remote document store: Disabled"
            }
            add(remoteStatusLabel, BorderLayout.EAST)
        }

        // Remote connection action using Utils.actionFu
        val remoteConnectionAction = Utils.actionFu(
            if (remoteStore == null) "Connect to Remote Store" else "Disconnect from Remote Store"
        ) {
            if (remoteStore == null) {
                // Enable remote store in preferences and restart required
                preferences.node(Constants.REMOTE_STORE_NODE).putBoolean(Constants.REMOTE_STORE_ENABLED_KEY, true)
                JOptionPane.showMessageDialog(
                    mainFrame,
                    "Remote document store has been enabled.\nPlease restart the application to connect.",
                    "Restart Required",
                    JOptionPane.INFORMATION_MESSAGE
                )
            } else {
                // Disable remote store in preferences and restart required
                preferences.node(Constants.REMOTE_STORE_NODE).putBoolean(Constants.REMOTE_STORE_ENABLED_KEY, false)
                JOptionPane.showMessageDialog(
                    mainFrame,
                    "Remote document store has been disabled.\nPlease restart the application to disconnect.",
                    "Restart Required",
                    JOptionPane.INFORMATION_MESSAGE
                )
            }
        }
        
        val toolsMenu: JMenu = JMenu("File").apply {
            add(Utils.actionFu("Preferences") {
                preferencesDialog.isVisible = true
            })
            add(remoteConnectionAction)
            
            // Add action to select document store
            add(Utils.actionFu("Select Document Store") {
                val options = arrayOf("Local Store", "Remote Store")
                val initialSelection = if (selectedDocStore == docStore) 0 else 1
                
                val selection = JOptionPane.showOptionDialog(
                    mainFrame,
                    "Select which document store to use:",
                    "Document Store Selection",
                    JOptionPane.DEFAULT_OPTION,
                    JOptionPane.QUESTION_MESSAGE,
                    null,
                    options,
                    options[initialSelection]
                )
                
                if (selection == 0) {
                    // Local store selected
                    selectedDocStore = docStore
                    JOptionPane.showMessageDialog(
                        mainFrame,
                        "Local document store selected.",
                        "Document Store Selection",
                        JOptionPane.INFORMATION_MESSAGE
                    )
                } else if (selection == 1 && remoteStore != null) {
                    // Remote store selected
                    selectedDocStore = remoteStore
                    JOptionPane.showMessageDialog(
                        mainFrame,
                        "Remote document store selected.",
                        "Document Store Selection",
                        JOptionPane.INFORMATION_MESSAGE
                    )
                } else if (selection == 1) {
                    // Remote store selected but not available
                    JOptionPane.showMessageDialog(
                        mainFrame,
                        "Remote document store is not available.\nPlease connect to a remote store first.",
                        "Document Store Selection",
                        JOptionPane.WARNING_MESSAGE
                    )
                }
            })
            
            // Add action to copy between document stores
            add(Utils.actionFu("Copy Document Store") {
                if (remoteStore == null) {
                    JOptionPane.showMessageDialog(
                        mainFrame,
                        "Remote document store is not available.\nPlease connect to a remote store first.",
                        "Copy Failed",
                        JOptionPane.WARNING_MESSAGE
                    )
                } else {
                    // Create and show the document store copy helper
                    com.ddougher.market.application.DocumentStoreCopyHelper(this@Application).showCopyDialog()
                }
            })
            
            addSeparator()
            add(Utils.actionFu("Backfill Common Stock Tickers") {
                if (selectedDocStore == docStore) {
                    GlobalScope.launch {
                        BackfilTickers(docStore, preferences.node("Polygon").get("apiKey", "unknown")).getCommonStocks()
                    }
                } else {
                    JOptionPane.showMessageDialog(
                        mainFrame,
                        "Backfill operation is only available with the local document store.\nPlease select the local document store first.",
                        "Operation Not Available",
                        JOptionPane.WARNING_MESSAGE
                    )
                }
            })
            add(Utils.actionFu("Backfill from csv") {
                GlobalScope.launch {
                    CSVImporter(this@Application).doImport()
                }
            })
            add(Utils.actionFu("Browse Data") {
                JDialog(mainFrame,"Data Browser", false).apply {
                    contentPane.add(BorderLayout.CENTER, StockDataBrowser(selectedDocStore.get(Stocks::class.java, "stocks")))
                    preferredSize = Dimension(1200, 900)
                    pack()
                    isVisible = true
                }
            })
        }

        val mainMenuBar = JMenuBar().apply {
            add(toolsMenu)
        }

        val mainFrame = JFrame("Dana Trade 2.0").apply {
            JFrame.setDefaultLookAndFeelDecorated(true)
            defaultCloseOperation = JFrame.DO_NOTHING_ON_CLOSE
            preferredSize = Dimension(1500, 500)
            addWindowListener(object : WindowAdapter() {
                override fun windowClosing(e: WindowEvent) { this@Application.stop() }
            })
            rootPane.contentPane.apply {
                add(BorderLayout.CENTER, desktopPane)
                add(BorderLayout.SOUTH, statusBar)
            }
            rootPane.jMenuBar = mainMenuBar
            pack()
        }

        val preferencesDialog = JDialog(mainFrame, "Preferences", false).apply {
            layout = BorderLayout()
            add(PreferencesEditor(preferences), BorderLayout.CENTER)
        }

    }
    val view = View()

    fun start() {
        SwingUtilities.invokeLater {
            view.mainFrame.isVisible = true
        }
    }

    /**
     * Gets the remote document store if it's enabled and initialized
     * 
     * @return The remote document store or null if not enabled
     */
    fun getRemoteDocumentStore(): DocumentStore? {
        return remoteStore
    }
    
    fun stop() {
        // Close the local document store
        docStore.close()
        preferences.node(Constants.DOC_STORE_NODE).get(Constants.DOC_STORE_BASE_PATH_KEY, Constants.DOC_STORE_DEFAULT_FOLDER_NAME).also { path ->
            @Suppress("ComplexRedundantLet")
            File(path).apply { mkdirs() }.let { File(it, "Database.dt1") }.apply {
                ObjectOutputStream(BufferedOutputStream(outputStream(), 65536)).use { it.writeObject(docStore) }
            }
        }
        println("Local document store closed")
        
        // Close the remote document store if it exists
        remoteStore?.let {
            if (it is RemoteDocumentStoreClient) {
                it.close()
                println("Remote document store connection closed")
            }
        }
        
        SwingUtilities.invokeLater {
            view.mainFrame.dispose()
        }
    }

}

fun main() {
    UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
    Application().start()
}