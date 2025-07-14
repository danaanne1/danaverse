package com.ddougher.market.application

import com.ddougher.market.Application
import com.ddougher.market.data.core.Equity
import com.ddougher.market.data.core.Stocks
import com.ddougher.market.data.extensions.mergeAggregateData
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import java.io.File
import java.io.FileInputStream
import java.util.zip.GZIPInputStream
import java.io.BufferedReader
import java.io.InputStreamReader
import javax.swing.SwingUtilities
import javax.swing.JFileChooser

/**
 * A class for importing CSV data into the application's document store.
 *
 * @property app The application instance that provides access to the document store and preferences.
 */
class CSVImporter(val app: Application) {

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
    suspend fun doImport() {
        getDirectoryPath()?.let { path ->
            doImportFrom(path)
        }
    }


    /**
     * Imports CSV data from the specified directory path into the application's document store.
     * Processes each file in the directory, extracts stock data, and saves it to the document store.
     *
     * @param directoryPath The path to the directory containing CSV files to import
     */
    suspend fun doImportFrom(directoryPath: String) {
        processAllRecordsIn(directoryPath).collect { values ->
            app.docStore.transact { ds ->
                val stocks: Stocks = ds.get(Stocks::class.java, "stocks")
                var equity: Equity? = null
                for (valueSet in values) {
                    if (equity == null || equity.symbol != valueSet["ticker"]) {
                        if (equity != null) { ds.put(equity) }
                        equity = stocks.tickers().getOrPut(valueSet["ticker"]!!) { ds.newInstance(Equity::class.java).also { ds.put(stocks) }}
                    }
                    equity!!.mergeAggregateData(
                        mapOf(
                            "o" to valueSet["open"]!!.toDouble(),
                            "h" to valueSet["high"]!!.toDouble(),
                            "l" to valueSet["low"]!!.toDouble(),
                            "c" to valueSet["close"]!!.toDouble(),
                            "v" to valueSet["volume"]!!.toLong(),
                            "vw" to 0.0,
                            "z" to valueSet["transactions"]!!.toLong(),
                            "s" to valueSet["window_start"]!!.toLong()
                        )
                    )
                }
                ds.put(equity)
            }
        }
    }
    
    
    

    /**
     * Processes all CSV files in the specified directory and groups records by ticker symbol.
     * 
     * @param directoryPath The path to the directory containing CSV files
     * @return A Flow emitting lists of data records, where each list contains records for a single ticker
     */
    suspend fun processAllRecordsIn(directoryPath: String): Flow<List<Map<String, String>>> = flow {
        listfiles(directoryPath).collect { filePath ->
            var currentTicker: String? = null
            val currentList = mutableListOf<Map<String, String>>()

            processFile(filePath).collect { values ->
                val ticker = values["ticker"]
                if (currentTicker != ticker && currentTicker != null) {
                    emit(currentList.toList())
                    currentList.clear()
                }
                currentTicker = ticker
                currentList.add(values)
            }
            if (currentList.isNotEmpty()) {
                emit(currentList.toList())
            }
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