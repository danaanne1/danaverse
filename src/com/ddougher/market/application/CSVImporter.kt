package com.ddougher.market.application

import com.ddougher.market.Application
import com.ddougher.market.data.core.Equity
import com.ddougher.market.data.core.Stocks
import com.ddougher.market.data.extensions.mergeAggregateData
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.io.File
import java.io.FileInputStream
import java.util.zip.GZIPInputStream
import java.io.BufferedReader
import java.io.InputStreamReader
import java.util.Calendar
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.Executors
import javax.swing.SwingUtilities
import javax.swing.JFileChooser

/**
 * A class for importing CSV data into the application's document store.
 *
 * @property app The application instance that provides access to the document store and preferences.
 */
class CSVImporter(val app: Application) {
    val tp = Executors.newCachedThreadPool().asCoroutineDispatcher()

    /**
     * Opens a directory chooser dialog and returns the selected directory path.
     *
     * @return The absolute path of the selected directory, or null if no directory was selected
     */

    private fun getDirectoryPath(): String? {
        var selectedPath: String? = null
        val fileChooser = JFileChooser().apply {
            fileSelectionMode = JFileChooser.DIRECTORIES_ONLY
            dialogTitle = "Select Directory"
        }
        if (fileChooser.showOpenDialog(null) == JFileChooser.APPROVE_OPTION) {
            selectedPath = fileChooser.selectedFile.absolutePath
        }
        return selectedPath
    }

    /**
     * Initiates the CSV import process by prompting the user to select a directory
     * and then importing data from the selected directory.
     */
    fun doImport() {
        getDirectoryPath()?.let { path ->
            doImportFrom(path)
        }
    }


    /**
     * Imports CSV data from the specified directory path into the application's document store using concurrent processing.
     * 
     * This method performs the following operations:
     * - Creates a bounded channel for processing batched CSV records
     * - Launches 100 concurrent coroutines to process equity data in parallel
     * - Uses per-ticker mutexes to ensure thread-safe updates to individual equities
     * - Processes each batch of records grouped by ticker symbol
     * - Merges aggregate OHLCV (Open, High, Low, Close, Volume) data into equity documents
     * - Updates the stocks registry with processed tickers
     * - Executes all operations within a document store transaction
     * 
     * The method expects CSV files to contain the following columns:
     * - ticker: Stock symbol identifier
     * - open, high, low, close: Price data as doubles
     * - volume: Trading volume as long
     * - transactions: Number of transactions as long  
     * - window_start: Timestamp in nanoseconds, converted to milliseconds for storage
     * 
     * Processing is done concurrently with proper synchronization:
     * - Individual ticker processing is serialized using per-ticker mutexes
     * - Global stocks registry updates are synchronized using a shared mutex
     * - Records are processed in batches grouped by ticker to optimize database operations
     * 
     * @param directoryPath The path to the directory containing CSV files to import
     * @throws Exception if CSV processing fails, database transaction fails, or concurrent access issues occur
     * 
     * @see processAllRecordsIn for CSV file processing details
     * @see Equity.mergeAggregateData for data merging logic
     */
    fun doImportFrom(directoryPath: String) {
        val recordChannel = Channel<List<Map<String, String>>>(100)

        app.selectedDocStore.transact { ds ->
            runBlocking {
                coroutineScope {
                    val stocksLock = Mutex()
                    val equityLock = ConcurrentHashMap<String, Mutex>()
                    repeat(100) {
                        launch(tp) {
                            do {
                                val values = recordChannel.receiveCatching().getOrNull()
                                while (values != null) {
                                    equityLock.getOrPut(values.first()["ticker"]!!) { Mutex() }.withLock {
                                        val equity =
                                            ds.get(Equity::class.java, "equities/${values.first()["ticker"]!!}")
                                                .also {
                                                    it.symbol = values.first()["ticker"]!!
                                                }

                                        for (valueSet in values) {
                                            equity!!.mergeAggregateData(
                                                mapOf(
                                                    "o" to valueSet["open"]!!.toDouble(),
                                                    "h" to valueSet["high"]!!.toDouble(),
                                                    "l" to valueSet["low"]!!.toDouble(),
                                                    "c" to valueSet["close"]!!.toDouble(),
                                                    "v" to valueSet["volume"]!!.toLong(),
                                                    "vw" to 0.0,
                                                    "z" to valueSet["transactions"]!!.toLong(),
                                                    "s" to valueSet["window_start"]!!.toLong() / 1000000
                                                )
                                            )
                                        }

                                        println(
                                            "${equity.symbol} ${
                                                Calendar.getInstance().apply {
                                                    timeInMillis =
                                                        values.first()["window_start"]!!.toLong() / 1000000
                                                }.time
                                            }"
                                        )

                                        ds.put(equity)
                                    }
                                    stocksLock.withLock {
                                        val stocks = ds.get(Stocks::class.java, "stocks")
                                        stocks.tickers().getOrPut(values.first()["ticker"]!!) {
                                            ds.get(Equity::class.java, "equities/${values.first()["ticker"]!!}")
                                                .also {
                                                    ds.put(stocks)
                                                }
                                        }
                                    }
                                    break
                                }
                            } while (values != null)
                        }
                    }
                    processAllRecordsIn(directoryPath).collect { recordChannel.send(it) }
                    recordChannel.close()
                }
            }
        }
        println("Done importing")
    }
    
    
    

    /**
     * Processes all CSV files in the specified directory and groups records by ticker symbol.
     * 
     * @param directoryPath The path to the directory containing CSV files
     * @return A Flow emitting lists of data records, where each list contains records for a single ticker
     */
    suspend fun processAllRecordsIn(directoryPath: String): Flow<List<Map<String, String>>> = channelFlow {
        coroutineScope {
            listfiles(directoryPath).map { filePath ->
                async {
                    var currentTicker: String? = null
                    val currentList = mutableListOf<Map<String, String>>()
                    processFile(filePath).collect { values ->
                        val ticker = values["ticker"]
                        if (currentTicker != ticker && currentTicker != null) {
                            send(currentList.toList())
                            currentList.clear()
                        }
                        currentTicker = ticker
                        currentList.add(values)
                    }
                    if (currentList.isNotEmpty()) {
                        send(currentList.toList())
                    }
                }
            }.toList().awaitAll()
        }
    }
    
    /**
     * Lists all files in the specified directory and its subdirectories recursively.
     *
     * @param directoryPath The path to the directory to scan
     * @return A Flow emitting absolute paths of all files found
     */
    fun listfiles(directoryPath: String): Flow<String> = flow {
        File(directoryPath).walkTopDown().filter { it.isFile }.forEach { file ->
            emit(file.absolutePath)
        }
    }

    /**
     * Processes a gzipped CSV file and converts each line to a map of column name to value.
     * The first line of the CSV file is expected to contain the column headers.
     *
     * @param filePath Path to the gzipped CSV file
     * @return A Flow emitting maps where keys are column names and values are the corresponding cell values
     */
    fun processFile(filePath: String): Flow<Map<String, String>> {
        return flow {
            GZIPInputStream(FileInputStream(filePath)).bufferedReader().use { reader ->
                val headers = reader.readLine()?.split(",")?.map { it.trim() } ?: return@flow
                reader.lineSequence().forEach { line ->
                    val values = line.split(",").map { it.trim() }
                    if (values.size == headers.size) {
                        emit(headers.zip(values).toMap())
                    }
                }
            }
        }
    }
    
}