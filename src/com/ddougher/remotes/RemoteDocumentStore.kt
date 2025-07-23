package com.ddougher.remotes

import com.ddougher.proxamic.Document
import com.ddougher.proxamic.DocumentStore
import com.ddougher.proxamic.DocumentView
import com.ddougher.proxamic.MemoryMappedDocumentStore
import java.io.*
import java.util.*
import java.util.function.Consumer

/**
 * A document store implementation that runs on a remote server.
 * All instances delegate to a singleton MemoryMappedDocumentStore.
 */
class RemoteDocumentStore(
    private val storePath: String
) : DocumentStore {
    
    /**
     * Public access to the singleton document store
     */
    val documentStore: MemoryMappedDocumentStore
        get() = DocumentStoreSingleton.getInstance(storePath)
    
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
    

    /**
     * Singleton implementation for the document store
     */
    private object DocumentStoreSingleton {
        @Volatile
        private var instance: MemoryMappedDocumentStore? = null
        
        fun getInstance(path: String): MemoryMappedDocumentStore {
            return instance ?: synchronized(this) {
                instance ?: initializeDocumentStore(path).also { instance = it }
            }
        }
        
        private fun initializeDocumentStore(path: String): MemoryMappedDocumentStore {
            val file = File(path, "Database.dt1")
            return if (file.exists()) {
                ObjectInputStream(BufferedInputStream(FileInputStream(file), 65536)).use { ois ->
                    ois.readObject() as MemoryMappedDocumentStore
                }
            } else {
                MemoryMappedDocumentStore(
                    Optional.of(path),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty()
                )
            }
        }
    }
}