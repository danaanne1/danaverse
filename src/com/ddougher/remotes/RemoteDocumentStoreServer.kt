package com.ddougher.remotes

import com.ddougher.market.Constants
import com.ddougher.remoting.GridServer
import java.io.File
import java.net.InetSocketAddress
import java.util.prefs.Preferences

/**
 * Standalone server for the remote document store.
 * This class provides a main method to start a GridServer that hosts
 * a RemoteDocumentStore instance.
 */
class RemoteDocumentStoreServer {
    companion object {
        /**
         * Main entry point for the remote document store server
         * 
         * @param args Command line arguments:
         *             [0] - Optional: port number (default: 3262)
         *             [1] - Optional: store directory path (default: from preferences or Constants)
         */
        @JvmStatic
        fun main(args: Array<String>) {
            // Parse command line arguments
            val port = if (args.isNotEmpty()) args[0].toInt() else 3262
            
            // Get store directory from arguments or preferences
            val storeDirectory = if (args.size > 1) {
                args[1]
            } else {
                // Try to get from preferences
                val preferences = Preferences.userNodeForPackage(RemoteDocumentStoreServer::class.java)
                preferences.node(Constants.DOC_STORE_NODE).get(
                    Constants.REMOTE_STORE_DIRECTORY_KEY,
                    Constants.DOC_STORE_DEFAULT_FOLDER_NAME + File.separator + "remote"
                )
            }
            
            // Ensure the store directory exists
            File(storeDirectory).mkdirs()
            
            // Create and start the server
            println("Starting Remote Document Store Server on port $port")
            println("Store directory: $storeDirectory")
            println("Available processors: ${Runtime.getRuntime().availableProcessors()}")
            println("Max memory: ${Runtime.getRuntime().maxMemory() / (1024 * 1024)} MB")
            
            val serverAddress = InetSocketAddress(port)
            val server = GridServer(serverAddress)
            
            // Add shutdown hook to gracefully stop the server
            Runtime.getRuntime().addShutdownHook(Thread {
                println("Shutting down Remote Document Store Server...")
                server.shutdown()
                println("Server shutdown complete")
            })
            
            // Start the server and wait
            server.start()
            println("Server started successfully. Press Ctrl+C to stop.")
            
            // Keep the main thread alive
            try {
                Thread.currentThread().join()
            } catch (e: InterruptedException) {
                // Shutdown initiated
                server.shutdown()
            }
        }
    }
}