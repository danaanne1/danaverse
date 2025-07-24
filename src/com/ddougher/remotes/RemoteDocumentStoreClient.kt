package com.ddougher.remotes

import com.ddougher.proxamic.Document
import com.ddougher.proxamic.DocumentStore
import com.ddougher.proxamic.DocumentStoreAware
import com.ddougher.proxamic.DocumentView
import com.ddougher.remoting.GridClient
import java.net.InetSocketAddress
import java.util.function.Consumer
import java.util.stream.Stream

/**
 * Client-side wrapper for RemoteDocumentStore that automatically sets
 * the document store reference on retrieved objects
 */
class RemoteDocumentStoreClient(
    private val serverAddress: InetSocketAddress,
    private val remoteStoreDirectory: String
) : DocumentStore {
    private val client: GridClient = GridClient(serverAddress).apply { start() }
    private val remoteStore: IRemoteDocumentStore = client.createRemoteObject(
        IRemoteDocumentStore::class.java,
        RemoteDocumentStore::class.java,
        arrayOf(String::class.java),
        arrayOf(remoteStoreDirectory)
    )
    

    override fun get(key: String): Document {
        val result = remoteStore.get(key)
        if (result is DocumentStoreAware) {
            result.setDocumentStore(this)
        }
        return result
    }
    

    override fun lock(key: String): Document {
        val result = remoteStore.lock(key)
        if (result is DocumentStoreAware) {
            result.setDocumentStore(this)
        }
        return result
    }
    


    override fun newInstance(): Document {
        val result = remoteStore.newInstance()
        if (result is DocumentStoreAware) {
            result.setDocumentStore(this)
        }
        return result
    }
    
    override fun newInstance(key: String): Document {
        val result = remoteStore.newInstance(key)
        if (result is DocumentStoreAware) {
            result.setDocumentStore(this)
        }
        return result
    }
    
    // Delegate other methods directly to the remote store
    override fun getID(document: Document): String {
        // Ensure document has the correct document store reference before sending
        if (document is DocumentStoreAware) {
            document.setDocumentStore(this)
        }
        return remoteStore.getID(document)
    }
    override fun release(document: Document) {
        // Ensure document has the correct document store reference before sending
        if (document is DocumentStoreAware) {
            document.setDocumentStore(this)
        }
        remoteStore.release(document)
    }
    override fun put(document: Document) {
        // Ensure document has the correct document store reference before sending
        if (document is DocumentStoreAware) {
            document.setDocumentStore(this)
        }
        remoteStore.put(document)
    }
    override fun delete(document: Document) {
        // Ensure document has the correct document store reference before sending
        if (document is DocumentStoreAware) {
            document.setDocumentStore(this)
        }
        remoteStore.delete(document)
    }

    override fun traverseKeys(startKey: String?, endKey: String?): Stream<String?>? {
        return remoteStore.keys(startKey, endKey).stream() as Stream<String?>?
    }

    // Close the client connection when done
    fun close() {
        client.close()
    }
}