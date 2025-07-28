package com.ddougher.market.application

import com.ddougher.documentstore.Document
import com.ddougher.market.Application
import com.ddougher.documentstore.DocumentStore
import com.ddougher.documentstore.MemoryMappedDocumentStore
import com.ddougher.documentstore.MemoryMappedDocumentStore.MemoryMappedDocument
import com.ddougher.market.Constants
import com.ddougher.remoting.GridClient
import com.ddougher.remoting.GridContext
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import java.awt.BorderLayout
import java.awt.Dimension
import java.awt.FlowLayout
import java.awt.GridBagConstraints
import java.awt.GridBagLayout
import java.awt.Insets
import java.awt.event.WindowAdapter
import java.awt.event.WindowEvent
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.net.InetSocketAddress
import java.util.Optional
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import javax.swing.BorderFactory
import javax.swing.JButton
import javax.swing.JDialog
import javax.swing.JLabel
import javax.swing.JOptionPane
import javax.swing.JPanel
import javax.swing.JProgressBar
import javax.swing.SwingUtilities

/**
 * Helper class for copying documents between document stores (local to remote and vice versa).
 * Provides a UI with progress tracking.
 *
 * @property app The application instance that provides access to document stores
 */
class DocumentStoreCopyHelper(private val app: Application) {

    /**
     * Interface for progress tracking during the copy process
     */
    interface CopyProgressListener {
        /**
         * Called when the copy process starts
         * @param totalDocuments The total number of documents to copy
         */
        fun onCopyStart(totalDocuments: Int)
        
        /**
         * Called when a document is copied
         * @param currentDocument The current document being copied
         * @param currentCount The number of documents copied so far
         * @param totalDocuments The total number of documents to copy
         */
        fun onDocumentCopied(currentDocument: String, currentCount: Int, totalDocuments: Int)
        
        /**
         * Called when the copy process completes
         * @param totalCopied The total number of documents copied
         */
        fun onCopyComplete(totalCopied: Int)
        
        /**
         * Called when an error occurs during the copy process
         * @param document The document that caused the error
         * @param error The error that occurred
         */
        fun onCopyError(document: String, error: Throwable)
    }
    
    /**
     * Shows the document store copy dialog
     */
    fun showCopyDialog() {
        val dialog = CopyDialog(app)
        dialog.isVisible = true
    }
    
    /**
     * Dialog for copying documents between stores
     */
    private inner class CopyDialog(private val app: Application) : JDialog(app.view.mainFrame, "Copy Document Store", true) {
        private val progressBar = JProgressBar(0, 100)
        private val statusLabel = JLabel("Ready to copy")
        private val detailLabel = JLabel("")
        private val copyLocalToRemoteButton = JButton("Copy Local to Remote")
        private val copyRemoteToLocalButton = JButton("Copy Remote to Local")
        private val cancelButton = JButton("Cancel")
        
        private var isCopying = false
        
        init {
            // Configure dialog
            defaultCloseOperation = DISPOSE_ON_CLOSE
            preferredSize = Dimension(500, 200)
            
            // Create UI components
            val contentPanel = JPanel(GridBagLayout())
            contentPanel.border = BorderFactory.createEmptyBorder(10, 10, 10, 10)
            
            val constraints = GridBagConstraints().apply {
                fill = GridBagConstraints.HORIZONTAL
                insets = Insets(5, 5, 5, 5)
                weightx = 1.0
            }
            
            // Add status label
            constraints.gridx = 0
            constraints.gridy = 0
            constraints.gridwidth = 2
            contentPanel.add(statusLabel, constraints)
            
            // Add progress bar
            constraints.gridy = 1
            contentPanel.add(progressBar, constraints)
            
            // Add detail label
            constraints.gridy = 2
            contentPanel.add(detailLabel, constraints)
            
            // Add buttons
            val buttonPanel = JPanel(FlowLayout(FlowLayout.RIGHT))
            buttonPanel.add(copyLocalToRemoteButton)
            buttonPanel.add(copyRemoteToLocalButton)
            buttonPanel.add(cancelButton)
            
            constraints.gridy = 3
            contentPanel.add(buttonPanel, constraints)
            
            // Add content panel to dialog
            contentPane.add(contentPanel, BorderLayout.CENTER)
            
            // Configure button actions
            copyLocalToRemoteButton.addActionListener {
                if (!isCopying) {
//                    if (app.remoteStore == null) {
//                        JOptionPane.showMessageDialog(
//                            this,
//                            "Remote document store is not available.\nPlease connect to a remote store first.",
//                            "Copy Failed",
//                            JOptionPane.WARNING_MESSAGE
//                        )
//                        return@addActionListener
//                    }

                    startCopy(app.docStore, app.remoteStore)
                }
            }
            
            copyRemoteToLocalButton.addActionListener {
                if (!isCopying) {
//                    if (app.remoteStore == null) {
//                        JOptionPane.showMessageDialog(
//                            this,
//                            "Remote document store is not available.\nPlease connect to a remote store first.",
//                            "Copy Failed",
//                            JOptionPane.WARNING_MESSAGE
//                        )
//                        return@addActionListener
//                    }
                    
                    startCopy(app.remoteStore!!, app.docStore)
                }
            }
            
            cancelButton.addActionListener {
                if (isCopying) {
                    // TODO: Implement cancellation logic
                    statusLabel.text = "Cancelling..."
                } else {
                    dispose()
                }
            }
            
            // Handle window close
            addWindowListener(object : WindowAdapter() {
                override fun windowClosing(e: WindowEvent) {
                    if (isCopying) {
                        val confirm = JOptionPane.showConfirmDialog(
                            this@CopyDialog,
                            "A copy operation is in progress. Are you sure you want to cancel?",
                            "Cancel Copy",
                            JOptionPane.YES_NO_OPTION
                        )
                        
                        if (confirm == JOptionPane.YES_OPTION) {
                            // TODO: Implement cancellation logic
                            dispose()
                        }
                    } else {
                        dispose()
                    }
                }
            })
            
            pack()
            setLocationRelativeTo(app.view.mainFrame)
        }
        
        /**
         * Starts the copy process from source to destination
         */
        private fun startCopy(source: DocumentStore, destination: DocumentStore?) {
            isCopying = true
            copyLocalToRemoteButton.isEnabled = false
            copyRemoteToLocalButton.isEnabled = false
            cancelButton.text = "Cancel"
            
            val progressListener = object : CopyProgressListener {
                override fun onCopyStart(totalDocuments: Int) {
                    SwingUtilities.invokeLater {
                        progressBar.maximum = totalDocuments
                        progressBar.value = 0
                        statusLabel.text = "Copying documents: 0/$totalDocuments"
                        detailLabel.text = "Starting copy..."
                    }
                }
                
                override fun onDocumentCopied(currentDocument: String, currentCount: Int, totalDocuments: Int) {
                    SwingUtilities.invokeLater {
                        progressBar.value = currentCount
                        statusLabel.text = "Copying documents: $currentCount/$totalDocuments"
                        detailLabel.text = "Copying: $currentDocument"
                    }
                }
                
                override fun onCopyComplete(totalCopied: Int) {
                    SwingUtilities.invokeLater {
                        progressBar.value = progressBar.maximum
                        statusLabel.text = "Copy complete: $totalCopied documents copied"
                        detailLabel.text = "Done"
                        isCopying = false
                        copyLocalToRemoteButton.isEnabled = true
                        copyRemoteToLocalButton.isEnabled = true
                        cancelButton.text = "Close"
                    }
                }
                
                override fun onCopyError(document: String, error: Throwable) {
                    SwingUtilities.invokeLater {
                        detailLabel.text = "Error copying $document: ${error.message}"
                        JOptionPane.showMessageDialog(
                            this@CopyDialog,
                            "Error copying document $document: ${error.message}",
                            "Copy Error",
                            JOptionPane.ERROR_MESSAGE
                        )
                    }
                }
            }
            
            GlobalScope.launch {
                try {
                    copyDocuments(source, destination, progressListener)
                } catch (e: Exception) {
                    SwingUtilities.invokeLater {
                        JOptionPane.showMessageDialog(
                            this@CopyDialog,
                            "Error during copy process: ${e.message}",
                            "Copy Failed",
                            JOptionPane.ERROR_MESSAGE
                        )
                        isCopying = false
                        copyLocalToRemoteButton.isEnabled = true
                        copyRemoteToLocalButton.isEnabled = true
                        cancelButton.text = "Close"
                    }
                }
            }
        }
        
        /**
         * Copies documents from source to destination
         * This is a placeholder implementation - the actual copy logic will be implemented by the user
         */
        private suspend fun copyDocuments(source: DocumentStore, destination: DocumentStore?, listener: CopyProgressListener) {
            if (destination == null) {
                copyDocumentsToRemote(source,  listener)
            } else {
                throw Exception("Destination store is not a MemoryMappedDocumentStore")
            }
        }


        @Suppress("ControlFlowWithEmptyBody")
        private suspend fun copyDocumentsToRemote(source: DocumentStore, listener: CopyProgressListener) {
            // Start the copy process
            val totalDocs = source.traverseKeys(null, null).count().toInt()
            listener.onCopyStart(totalDocs)
            val listenerMutex = Mutex()
            val disp = Executors.newCachedThreadPool()

            val count = AtomicInteger(0)
            withContext(disp.asCoroutineDispatcher()) {
                try {

                    val docChannel = Channel<Document>(1000)
                    repeat(4) {
                        launch {
                            val remoteServerHost = app.preferences.node(Constants.REMOTE_STORE_NODE)
                                .get(Constants.REMOTE_STORE_HOST_KEY, "localhost")
                            val remoteServerPort = app.preferences.node(Constants.REMOTE_STORE_NODE)
                                .getInt(Constants.REMOTE_STORE_PORT_KEY, 3262)
                            val remoteStoreDirectory = app.preferences.node(Constants.DOC_STORE_NODE).get(
                                Constants.REMOTE_STORE_DIRECTORY_KEY,
                                Constants.DOC_STORE_DEFAULT_FOLDER_NAME + File.separator + "remote"
                            )
                            // Create the remote document store client
                            val serverAddress = InetSocketAddress(remoteServerHost, remoteServerPort)
                            GridClient(serverAddress).apply { start() }.use { client ->
                                val copyHelper = client.createRemoteObject(
                                    RemoteCopyHelper::class.java,
                                    RemoteCopyHelperImpl::class.java,
                                    arrayOf<Class<*>>(String::class.java),
                                    arrayOf<Any>(remoteStoreDirectory)
                                )

                                val recs = mutableListOf<Document>()
                                while (null != docChannel.receiveCatching().getOrNull()?.also {
                                        recs.add(it)
                                        if (recs.size >= 50) {
                                            copyHelper.ingestDocuments(java.util.ArrayList(recs))
                                            listenerMutex.withLock {
                                                listener.onDocumentCopied("x", count.addAndGet(recs.size), totalDocs)
                                            }
                                            recs.clear()
                                        }
                                    });
                                if (recs.isNotEmpty()) {
                                    copyHelper.ingestDocuments(java.util.ArrayList(recs))
                                    listenerMutex.withLock {
                                        listener.onDocumentCopied("x", count.addAndGet(recs.size), totalDocs)
                                    }
                                }

                            }
                        }
                    }
                    source.traverseDocuments(null, null).parallel().forEach { runBlocking { docChannel.send(it) } }
                    docChannel.close()
                } catch ( ex: Exception ) {
                    ex.printStackTrace(System.err)
                }
            }
            listener.onCopyComplete(count.get())
            disp.shutdown()
            println("All Done!")
        }
    }
}

interface RemoteCopyHelper {
    fun ingestDocuments(docs: java.util.ArrayList<Document>)
}

class RemoteCopyHelperImpl(val storePath:String): RemoteCopyHelper {
    val documentStore = GridContext.context.getOrPut("DanaMarketData") {
        val file = File(storePath, "Database.dt1")
        (if (file.exists()) {
            ObjectInputStream(BufferedInputStream(FileInputStream(file), 65536)).use { ois ->
                ois.readObject() as MemoryMappedDocumentStore
            }
        } else {
            MemoryMappedDocumentStore(
                Optional.of(storePath),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty()
            )
        }).also {
            Runtime.getRuntime().addShutdownHook(Thread {
                it.close()
                ObjectOutputStream(BufferedOutputStream(FileOutputStream(file), 65536)).use { ois ->
                    ois.writeObject(it)
                    ois.flush()
                }
            })
        }
    } as DocumentStore

    override fun ingestDocuments(docs: java.util.ArrayList<Document>) {
        docs.parallelStream().map {it.`as`(MemoryMappedDocument::class.java)}.forEach { doc ->
            documentStore.put(doc.withVERSION(documentStore.get(MemoryMappedDocument::class.java, doc.ID()).VERSION()))
        }
    }
}