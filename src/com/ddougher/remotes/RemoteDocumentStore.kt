package com.ddougher.remotes

import com.ddougher.documentstore.Document
import com.ddougher.documentstore.DocumentStore
import com.ddougher.documentstore.MemoryMappedDocumentStore
import com.ddougher.remoting.GridContext
import java.io.*
import java.util.*


interface  IRemoteDocumentStore: DocumentStore {
    fun keys(start: String?, end: String?): Set<String>
}

/**
 * A document store implementation that runs on a remote server.
 * All instances delegate to a singleton MemoryMappedDocumentStore.
 */
class RemoteDocumentStore(
    private val storePath: String
) : IRemoteDocumentStore {
    
    /**
     * Public access to the singleton document store
     */
    val documentStore = GridContext.context["DanaMarketData"]!! as MemoryMappedDocumentStore

    override fun getID(document: Document): String {
        return documentStore.getID(document)
    }
    
    override fun newInstance(): Document {
        return documentStore.newInstance()
    }
    
    override fun newInstance(key: String): Document {
        return documentStore.newInstance(key)
    }
    
    override fun get(key: String): Document {
        return documentStore.get(key)
    }
    
    override fun lock(key: String): Document {
        return documentStore.lock(key)
    }
    
    override fun release(document: Document) {
        documentStore.release(document)
    }
    
    override fun put(document: Document) {
        documentStore.put(document)
    }
    
    override fun delete(document: Document) {
        documentStore.delete(document)
    }

    override fun keys(start:String?, end:String?): Set<String> {
        val keys = documentStore.keys()
        if (keys.isEmpty()) return keys
        return keys.subSet(start?:keys.first, true, end?:keys.last, true)
    }

}