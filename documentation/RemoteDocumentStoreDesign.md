# Remote Document Store Design

## Overview

This document outlines the design for adding a remote document store capability to the Danaverse application. Unlike the previous remote document store design, this approach creates a second document store that is not automatically synchronized with the local store. The remote document store runs on a remote server and can be accessed by client applications.

### Objectives

1. **Remote Document Store**: Create a second document store that runs on a remote server.
2. **Multi-Process Access**: Allow multiple processes to access the remote document store.
3. **Independent Operation**: Operate independently from the local document store without automatic synchronization.
4. **Preference Configuration**: Integrate with the existing preferences system for configuration.

## Architecture

### Components

1. **Local Document Store**: The existing `MemoryMappedDocumentStore` that provides local persistence.
2. **Remote Document Store**: A document store implementation that runs on a remote server.
3. **Singleton Document Store**: A singleton implementation of `MemoryMappedDocumentStore` that is shared across all instances of the remote document store.
4. **Remote Document Store Proxy**: A client-side implementation of `DocumentStore` that forwards operations to the remote document store.
5. **GridClient/GridServer**: The existing remoting infrastructure used for communication.

### Component Interactions

```
┌─────────────────────┐      ┌─────────────────────┐      ┌─────────────────────┐
│                     │      │                     │      │                     │
│  Application Code   │─────▶│  Local Document     │      │  Remote Document    │
│                     │      │  Store              │      │  Store Proxy        │◀─┐
│                     │      │                     │      │                     │  │
└─────────────────────┘      └─────────────────────┘      └─────────────────────┘  │
                                                                     │             │
                                                                     │             │
                                                                     ▼             │
┌─────────────────────┐      ┌─────────────────────┐      ┌─────────────────────┐  │
│                     │      │                     │      │                     │  │
│  Remote Document    │◀─────│  GridServer         │◀─────│  GridClient         │──┘
│  Store              │      │                     │      │                     │
│                     │      │                     │      │                     │
└─────────────────────┘      └─────────────────────┘      └─────────────────────┘
          │
          │
          ▼
┌─────────────────────┐
│                     │
│  Singleton Document │
│  Store              │
│                     │
└─────────────────────┘
```

## Server-Side Implementation

### Remote Document Store

The remote document store will be implemented with a singleton pattern for the internal document store:

1. **Singleton Document Store**: The internal document store is implemented as a singleton, ensuring all instances share the same document store.
2. **Public Access**: The document store is publicly accessible, allowing direct access when needed.
3. **Shared Storage**: All instances access the same underlying storage location.
4. **Independent Operation**: While the RemoteDocumentStore instances are independent, they all delegate to the same singleton document store.

### RemoteDocumentStore

```kotlin
class RemoteDocumentStore(
    private val storePath: String
) : DocumentStore {
    
   // Public access to the singleton document store
   val documentStore: MemoryMappedDocumentStore
      get() = DocumentStoreSingleton.getInstance(storePath)
    
   // Implement all DocumentStore methods by delegating to the internal documentStore
   override fun get(type: Class<T>, key: String): T {
      return documentStore.get(type, key)
   }
    
   // ... other DocumentStore methods ...
   
   // Singleton implementation for the document store
   object DocumentStoreSingleton {
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
            ObjectInputStream(BufferedInputStream(file.inputStream(), 65536)).use {
               it.readObject() as MemoryMappedDocumentStore
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
```


## Client-Side Implementation

### Client-Side Delegation

To ensure proper document store references are maintained across the remote boundary, we need to implement client-side delegation that automatically sets the document store reference on retrieved objects:

```kotlin
/**
 * Client-side wrapper for RemoteDocumentStore that automatically sets
 * the document store reference on retrieved objects
 */
class RemoteDocumentStoreClient(
    private val serverAddress: InetSocketAddress,
    private val remoteStoreDirectory: String
) : DocumentStore {
    private val client: GridClient = GridClient(serverAddress).apply { start() }
    private val remoteStore: DocumentStore = client.createRemoteObject(
        DocumentStore::class.java,
        RemoteDocumentStore::class.java,
        arrayOf(String::class.java),
        arrayOf(remoteStoreDirectory)
    )
    
    // Override get methods to set document store reference on retrieved objects
    override fun <T : DocumentView> get(viewClass: Class<T>, key: String): T {
        val result = remoteStore.get(viewClass, key)
        result.setDocumentStore(this)
        return result
    }
    
    override fun get(key: String): Document {
        val result = remoteStore.get(key)
        if (result is DocumentStoreAware) {
            result.setDocumentStore(this)
        }
        return result
    }
    
    override fun <T : DocumentView> lock(viewClass: Class<T>, key: String): T {
        val result = remoteStore.lock(viewClass, key)
        result.setDocumentStore(this)
        return result
    }
    
    override fun lock(key: String): Document {
        val result = remoteStore.lock(key)
        if (result is DocumentStoreAware) {
            result.setDocumentStore(this)
        }
        return result
    }
    
    // For new instances, set the document store reference
    override fun <T : DocumentView> newInstance(viewClass: Class<T>): T {
        val result = remoteStore.newInstance(viewClass)
        result.setDocumentStore(this)
        return result
    }
    
    override fun <T : DocumentView> newInstance(viewClass: Class<T>, key: String): T {
        val result = remoteStore.newInstance(viewClass, key)
        result.setDocumentStore(this)
        return result
    }
    
    // Delegate other methods directly to the remote store
    override fun getID(document: Document): String = remoteStore.getID(document)
    
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
    
    override fun release(document: Document) = remoteStore.release(document)
    override fun put(document: Document) = remoteStore.put(document)
    override fun delete(document: Document) = remoteStore.delete(document)
    
    // Close the client connection when done
    fun close() {
        client.close()
    }
}
```

### Direct Proxy Instantiation

```kotlin
// Factory method to create a RemoteDocumentStoreClient
fun createRemoteDocumentStore(
    serverAddress: InetSocketAddress,
    remoteStoreDirectory: String
): DocumentStore {
    return RemoteDocumentStoreClient(serverAddress, remoteStoreDirectory)
}
```

## Integration with Existing System

### Application Integration

```kotlin
// In application startup code
// Load preferences
val preferences = Preferences.userNodeForPackage(javaClass).apply {
    // Initialize default preferences if not already set
    node("DocStore").apply { 
        put(Constants.DOC_STORE_BASE_PATH_KEY, get(Constants.DOC_STORE_BASE_PATH_KEY, Constants.DOC_STORE_DEFAULT_FOLDER_NAME))
        put("remoteStoreDirectory", get("remoteStoreDirectory", Constants.DOC_STORE_DEFAULT_FOLDER_NAME + File.separator + "remote"))
    }
    node("RemoteStore").apply { 
        put("host", get("host", "localhost"))
        putInt("port", getInt("port", 3262))
        putBoolean("enabled", getBoolean("enabled", false))
    }
}

// Local document store configuration (existing code)
val localStore: MemoryMappedDocumentStore = 
    preferences.node("DocStore").get(Constants.DOC_STORE_BASE_PATH_KEY, Constants.DOC_STORE_DEFAULT_FOLDER_NAME).let { path ->
        File(path).apply { mkdirs() }.let { File(it, "Database.dt1") }.let { file ->
            if (file.exists())
                ObjectInputStream(BufferedInputStream(file.inputStream(), 65536)).use { it.readObject() as MemoryMappedDocumentStore }
            else
                MemoryMappedDocumentStore(Optional.of(path), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty())
        }
    }

// Only initialize remote store if enabled
if (preferences.node("RemoteStore").getBoolean("enabled", false)) {
    // Remote document store configuration
    val remoteServerHost = preferences.node("RemoteStore").get("host", "localhost")
    val remoteServerPort = preferences.node("RemoteStore").getInt("port", 3262)
    val remoteStoreDirectory = preferences.node("DocStore").get("remoteStoreDirectory", Constants.DOC_STORE_DEFAULT_FOLDER_NAME + File.separator + "remote")
    val serverAddress = InetSocketAddress(remoteServerHost, remoteServerPort)

    // Create remote store with client-side delegation
    val remoteStore = createRemoteDocumentStore(
        serverAddress,
        remoteStoreDirectory
    )
    
    // The RemoteDocumentStoreClient automatically sets the document store reference
    // on all retrieved objects, eliminating the need for manual setDocumentStore calls
    
    // Make the remote store available to the application
    // This could be through a service locator, dependency injection, or direct reference
    application.setRemoteDocumentStore(remoteStore)
}

// Use the local store for local operations
// Use the remote store for remote operations when needed
```

## Preference Configuration

The remote document store configuration will integrate with the existing preferences system, which uses Java's Preferences API with hierarchical nodes. The following preferences will be added to the existing structure:

1. **DocStore Node** (existing):
   - **basePath**: Directory for the local document store (existing)
   - **remoteStoreDirectory**: Directory for the remote document store (new)

2. **RemoteStore Node** (new):
   - **host**: Hostname or IP address of the remote server
   - **port**: Port number for the remote server
   - **enabled**: Flag to enable/disable remote document store

These preferences will be accessible through the existing PreferencesEditor component, which already supports hierarchical preference nodes and key-value editing. The PreferencesEditor displays the preference nodes in a tree view and their key-value pairs in a table, allowing users to modify the values directly.

### Feature Toggling

The remote document store feature can be enabled or disabled through the preferences system:

1. **Default State**: The remote document store is disabled by default (`RemoteStore.enabled = false`).
2. **Enabling**: Users can enable the remote document store by setting `RemoteStore.enabled = true` in the preferences editor.
3. **Runtime Behavior**: The application checks this preference at startup to determine whether to initialize the remote document store.
4. **Immediate Effect**: Changes to this preference take effect after restarting the application.

This approach allows users to:
- Use the application in standalone mode without any remote connectivity
- Enable remote document store when needed
- Configure remote settings before enabling the feature
- Disable the remote document store temporarily without losing configuration

### Constants Extension

```kotlin
class Constants {
    companion object {
        // Existing constants
        val DOC_STORE_BASE_PATH_KEY = "basePath"
        val DOC_STORE_DEFAULT_FOLDER_NAME = System.getProperty("user.home") + File.separator + String.join(File.separator, "Documents", "DanaTrade", "Data")
        val REMOTE_SERVER_ENDPOINT_KEY = "remotes"
        
        // New constants for remote document store
        val REMOTE_STORE_DIRECTORY_KEY = "remoteStoreDirectory"
        val REMOTE_STORE_HOST_KEY = "host"
        val REMOTE_STORE_PORT_KEY = "port"
        val REMOTE_STORE_ENABLED_KEY = "enabled"
        
        // Node names
        val DOC_STORE_NODE = "DocStore"
        val REMOTE_STORE_NODE = "RemoteStore"
    }
}
```

## Error Handling and Recovery

### Connection Failures

1. **Retry Mechanism**: Failed remote operations will be retried with exponential backoff.
2. **Fallback to Local**: When remote operations fail, the application can fall back to local operations when appropriate.
3. **Health Monitoring**: The connection to the remote store will be monitored for health.

### Data Consistency

1. **Transactional Updates**: Updates to the remote store will be transactional when possible.
2. **Validation**: Document integrity will be validated before and after remote operations.
3. **Logging**: All remote operations will be logged for audit and debugging.

## Performance Considerations

### Optimization Strategies

1. **Connection Pooling**: Reuse connections to the remote store when possible.
2. **Batch Processing**: Group multiple document operations into batched updates when appropriate.
3. **Caching**: Cache frequently accessed remote documents locally to reduce network traffic.

## Future Enhancements

1. **Synchronization Capability**: Add optional synchronization between local and remote stores.
2. **Multiple Remote Stores**: Support connections to multiple remote document stores.
3. **Conflict Resolution**: Implement conflict resolution strategies for synchronized documents.
4. **Offline Mode**: Support working offline with automatic reconnection when available.
5. **Selective Document Access**: Allow configuring which documents or document types should be accessed remotely.

## Conclusion

This design provides a foundation for adding a remote document store capability to the Danaverse application. By implementing the document store on the remote server, we enable applications to access a shared data repository for distributed applications.

The client-side delegation approach ensures that document store references are automatically maintained across the remote boundary, eliminating the need for manual `setDocumentStore` calls that were previously required. For both DocumentView objects and Document objects that implement DocumentStoreAware, the client automatically sets the document store reference. This makes the remote document store more user-friendly and less error-prone.

The implementation follows the principle of making minimal necessary changes to the existing codebase while providing a powerful new capability. The remote document store operates independently from the local store, giving applications flexibility in how they manage and access data across different environments.