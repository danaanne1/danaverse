# Remote Document Store Design

## Overview

This document outlines the design for adding a remote document store capability to the Danaverse application. The design allows the existing local document store and a new remote document store to run simultaneously, with changes to the local store automatically propagated to the remote store in the background.

## Architecture

### Components

1. **Local Document Store**: The existing `MemoryMappedDocumentStore` that provides local persistence.
2. **Remote Document Store**: A document store implementation that runs on a remote server.
3. **Synchronization Manager**: A new component responsible for propagating changes from the local to the remote store.
4. **GridClient/GridServer**: The existing remoting infrastructure used for communication.

### Component Interactions

```
┌─────────────────────┐      ┌─────────────────────┐      ┌─────────────────────┐
│                     │      │                     │      │                     │
│  Application Code   │─────▶│  Local Document     │─────▶│  Synchronization    │
│                     │      │  Store              │      │  Manager            │
│                     │      │                     │      │                     │
└─────────────────────┘      └─────────────────────┘      └─────────────────────┘
                                                                     │
                                                                     │
                                                                     ▼
┌─────────────────────┐      ┌─────────────────────┐      ┌─────────────────────┐
│                     │      │                     │      │                     │
│  Remote Document    │◀─────│  GridClient         │◀─────│  Background         │
│  Store              │      │                     │      │  Sync Thread        │
│                     │      │                     │      │                     │
└─────────────────────┘      └─────────────────────┘      └─────────────────────┘
```

## Synchronization Mechanism

### Observer Pattern Implementation

The synchronization will leverage the existing Observable pattern in the document store:

1. The Synchronization Manager registers as an observer of the local document store.
2. When documents are modified in the local store, the Synchronization Manager is notified.
3. The Synchronization Manager queues these changes for background processing.
4. A background thread processes the queue and propagates changes to the remote store.

### Change Propagation

The synchronization process will follow these steps:

1. **Change Detection**: Detect changes to documents in the local store through the observer pattern.
2. **Change Queuing**: Queue changes for asynchronous processing.
3. **Background Processing**: Process queued changes in a background thread.
4. **Remote Update**: Send updates to the remote document store using the GridClient.
5. **Conflict Resolution**: Handle any conflicts that may arise during synchronization.

### Conflict Resolution

When conflicts occur between local and remote stores:

1. **Last-Writer-Wins**: By default, the local store's changes will overwrite remote changes.
2. **Version Tracking**: Document versions will be used to detect conflicts.
3. **Conflict Notification**: Application code can be notified of conflicts for manual resolution if needed.

## Implementation Details

### SynchronizationManager

<details>
<summary>SynchronizationManager: Manages document synchronization between local and remote stores using coroutines and channels</summary>

```kotlin
class SynchronizationManager(
    private val localStore: ObservableDocumentStore,
    private val remoteStore: DocumentStore,
    private val coroutineScope: CoroutineScope,
    private val numThreads: Int = 10
) : DocumentStoreObserver {
    
    private val changeChannel = Channel<DocumentChange>(Channel.UNLIMITED)
    private val dispatcher = Dispatchers.IO.limitedParallelism(numThreads)
    
    // Observable property for pending changes count
    private val _pendingChangesCount = MutableStateFlow(0)
    val pendingChangesCount: StateFlow<Int> = _pendingChangesCount.asStateFlow()
    
    init {
        localStore.addObserver(this)
        // Launch multiple coroutines to process the channel
        repeat(numThreads) {
            coroutineScope.launch(dispatcher) {
                processChangeChannel()
            }
        }
    }
    
    override fun onDocumentChanged(document: Any) {
        // Queue the document for synchronization - using runBlocking which may cause the application to become unresponsive
        runBlocking {
            changeChannel.send(DocumentChange(document))
            _pendingChangesCount.value = _pendingChangesCount.value + 1
        }
    }
    
    private suspend fun processChangeChannel() {
        for (change in changeChannel) {
            try {
                synchronizeDocument(change.document)
                // Decrement the pending changes count after successful synchronization
                _pendingChangesCount.value = _pendingChangesCount.value - 1
            } catch (e: Exception) {
                // Log error and continue
            }
        }
    }
    
    private suspend fun synchronizeDocument(document: Any) {
        try {
            // Use the remote store to update the document
            withContext(Dispatchers.IO) {
                remoteStore.put(document)
            }
        } catch (e: Exception) {
            // Handle synchronization errors
            // Possibly retry with exponential backoff
            // Log the failure
        }
    }
    
    fun shutdown() {
        changeChannel.close()
        localStore.removeObserver(this)
    }
    
    data class DocumentChange(val document: Any)
}
```
</details>

### RemoteDocumentStoreFactory

<details>
<summary>RemoteDocumentStoreFactory: Creates remote document store instances using GridClient</summary>

```kotlin
object RemoteDocumentStoreFactory {
    
    fun createRemoteDocumentStore(
        serverAddress: InetSocketAddress,
        remoteStoreDirectory: String
    ): DocumentStore {
        val client = GridClient(serverAddress)
        client.start()
        
        return client.createRemoteObject(
            DocumentStore::class.java,
            MemoryMappedDocumentStore::class.java,
            arrayOf(String::class.java),
            arrayOf(remoteStoreDirectory)
        )
    }
}
```
</details>

### Application Integration

<details>
<summary>Application Integration: Configures and initializes the remote document store and synchronization components</summary>

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
    node("Synchronization").apply { 
        putInt("threads", getInt("threads", 10))
        putInt("batchSize", getInt("batchSize", 50))
        putInt("retryDelay", getInt("retryDelay", 5000))
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

    // Create remote store
    val remoteStore = RemoteDocumentStoreFactory.createRemoteDocumentStore(
        serverAddress,
        remoteStoreDirectory
    )

    // Synchronization configuration
    val numSyncThreads = preferences.node("Synchronization").getInt("threads", 10)
    val coroutineScope = CoroutineScope(SupervisorJob())

    // Create synchronization manager
    val syncManager = SynchronizationManager(
        localStore,
        remoteStore,
        coroutineScope,
        numSyncThreads
    )

    // Register shutdown hook
    Runtime.getRuntime().addShutdownHook(Thread {
        runBlocking {
            coroutineScope.cancel()
            syncManager.shutdown()
        }
    })

    // Create and add status widget to the UI
    val statusWidget = SynchronizationStatusWidget(syncManager)
    view.mainFrame.statusBar.add(statusWidget, BorderLayout.EAST)
}

// Use the local store for all application operations
// Changes will be automatically synchronized to the remote store if enabled
```
</details>

## Preference Configuration

The remote document store configuration will integrate with the existing preferences system, which uses Java's Preferences API with hierarchical nodes. The following preferences will be added to the existing structure:

1. **DocStore Node** (existing):
   - **basePath**: Directory for the local document store (existing)
   - **remoteStoreDirectory**: Directory for the remote document store (new)

2. **RemoteStore Node** (new):
   - **host**: Hostname or IP address of the remote server
   - **port**: Port number for the remote server
   - **enabled**: Flag to enable/disable remote synchronization

3. **Synchronization Node** (new):
   - **threads**: Number of threads to use for synchronization (default: 10)
   - **batchSize**: Number of documents to batch in a single synchronization operation (default: 50)
   - **retryDelay**: Delay in milliseconds before retrying failed synchronizations (default: 5000)

These preferences will be accessible through the existing PreferencesEditor component, which already supports hierarchical preference nodes and key-value editing. The PreferencesEditor displays the preference nodes in a tree view and their key-value pairs in a table, allowing users to modify the values directly.

### Feature Toggling

The remote synchronization feature can be enabled or disabled through the preferences system:

1. **Default State**: The remote synchronization is disabled by default (`RemoteStore.enabled = false`).
2. **Enabling**: Users can enable synchronization by setting `RemoteStore.enabled = true` in the preferences editor.
3. **Runtime Behavior**: The application checks this preference at startup to determine whether to initialize the remote document store and synchronization manager.
4. **Immediate Effect**: Changes to this preference take effect after restarting the application.

This approach allows users to:
- Use the application in standalone mode without any remote connectivity
- Enable remote synchronization when needed
- Configure remote settings before enabling the feature
- Disable synchronization temporarily without losing configuration

### Constants Extension

<details>
<summary>Constants Extension: Adds new constants for remote document store configuration</summary>

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
        
        // New constants for synchronization
        val SYNC_THREADS_KEY = "threads"
        val SYNC_BATCH_SIZE_KEY = "batchSize"
        val SYNC_RETRY_DELAY_KEY = "retryDelay"
        
        // Node names
        val DOC_STORE_NODE = "DocStore"
        val REMOTE_STORE_NODE = "RemoteStore"
        val SYNCHRONIZATION_NODE = "Synchronization"
    }
}
```
</details>

## Error Handling and Recovery

### Connection Failures

1. **Retry Mechanism**: Failed synchronization attempts will be retried with exponential backoff.
2. **Queuing**: Changes will be queued during connection outages and processed when the connection is restored.
3. **Health Monitoring**: The connection to the remote store will be monitored for health.

### Data Consistency

1. **Transactional Updates**: Updates to the remote store will be transactional when possible.
2. **Validation**: Document integrity will be validated before and after synchronization.
3. **Logging**: All synchronization activities will be logged for audit and debugging.

## Performance Considerations

### Optimization Strategies

1. **Batch Processing**: Group multiple document changes into batched updates.
2. **Prioritization**: Prioritize critical document updates over less important ones.
3. **Throttling**: Limit synchronization rate to prevent overwhelming the remote store.
4. **Selective Synchronization**: Allow configuration of which document types should be synchronized.

### Resource Management

1. **Memory Usage**: Use unlimited capacity channels to ensure no document changes are lost, accepting potential memory growth.
2. **Synchronous Processing**: Use runBlocking for document change handling, accepting potential application unresponsiveness during high load.
3. **Coroutine Management**: Leverage Kotlin coroutines with a configurable thread pool for processing the channel.
4. **Connection Pooling**: Reuse connections to the remote store when possible.
5. **Structured Concurrency**: Use coroutine scopes and supervisors for proper lifecycle management.

## UI Components

### Synchronization Status Widget

<details>
<summary>SynchronizationStatusWidget: Displays the count of pending document changes in the application status bar</summary>

```kotlin
class SynchronizationStatusWidget(private val syncManager: SynchronizationManager) : JPanel() {
    
    private val pendingChangesLabel = JLabel("Pending Changes: 0")
    private val statusIndicator = JPanel().apply { 
        preferredSize = Dimension(16, 16)
        background = Color.GREEN
    }
    
    init {
        layout = BorderLayout(5, 0)
        add(statusIndicator, BorderLayout.WEST)
        add(pendingChangesLabel, BorderLayout.CENTER)
        border = BorderFactory.createEmptyBorder(2, 5, 2, 5)
        
        // Observe the pending changes count
        CoroutineScope(Dispatchers.Main).launch {
            syncManager.pendingChangesCount.collect { count ->
                SwingUtilities.invokeLater {
                    updateUI(count)
                }
            }
        }
    }
    
    private fun updateUI(count: Int) {
        pendingChangesLabel.text = "Pending Changes: $count"
        statusIndicator.background = when {
            count == 0 -> Color.GREEN
            count <= 10 -> Color.YELLOW
            else -> Color.ORANGE
        }
    }
}
```
</details>

## Future Enhancements

1. **Bidirectional Synchronization**: Support changes propagating from remote to local store.
2. **Multiple Remote Stores**: Support synchronization with multiple remote stores.
3. **Conflict Resolution Strategies**: Implement more sophisticated conflict resolution strategies.
4. **Offline Mode**: Support working offline with automatic synchronization when reconnected.
5. **Selective Synchronization**: Allow configuring which documents or document types should be synchronized.

## Conclusion

This design provides a foundation for adding remote document store capabilities to the Danaverse application. By leveraging the existing observer pattern and remoting infrastructure, we can implement a robust synchronization mechanism that keeps the local and remote document stores in sync with minimal impact on application performance and user experience.

The implementation follows the principle of making minimal necessary changes to the existing codebase while providing a powerful new capability. The background synchronization approach ensures that the application remains responsive even during synchronization operations.