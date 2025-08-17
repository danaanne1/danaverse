package com.ddougher.market.application

import com.ddougher.market.data.MetricConstants
import com.ddougher.market.data.core.Equity
import com.ddougher.market.data.core.Stocks
import java.awt.*
import java.text.SimpleDateFormat
import java.util.*
import javax.swing.*
import javax.swing.border.EmptyBorder
import javax.swing.border.TitledBorder
import javax.swing.event.ListSelectionEvent
import javax.swing.event.ListSelectionListener
import javax.swing.table.AbstractTableModel
import javax.swing.table.DefaultTableCellRenderer

/**
 * Stock data browser implementing the UI design for viewing and analyzing equity data
 */
class StockDataBrowser(val stocks: Stocks): JPanel(BorderLayout()) {
    /** 
     * Sorted list of ticker symbols for consistent display ordering
     */
    val tickerKeys = stocks.tickers().keys.toSortedSet().toList()

    /**
     * List model for displaying equity objects in JList
     * Provides access to equity objects by index from the sorted ticker keys
     */
    val equityListModel: AbstractListModel<Equity> = object: AbstractListModel<Equity>() {
        var s = stocks
        override fun getSize(): Int = s.tickers().size
        override fun getElementAt(index: Int): Equity = s.tickers()[tickerKeys[index]]!!
    }

    /**
     * Custom cell renderer for the equity list
     * Formats each equity with symbol in bold and name in smaller text
     */
    val cellRenderer = object: DefaultListCellRenderer() {
        override fun getListCellRendererComponent(list: JList<*>?, value: Any?, index: Int, isSelected: Boolean, cellHasFocus: Boolean): Component =
            when (value) {
                is Equity -> super.getListCellRendererComponent(list, "<html><b>${value.symbol}</b> <small>${value.name}</small></html>", index, isSelected, cellHasFocus)
                else -> super.getListCellRendererComponent(list, value, index, isSelected, cellHasFocus)
            }
    }

    /**
     * JList component for displaying and selecting stocks
     * Uses custom cell renderer and allows only single selection
     */
    val equityList = JList(equityListModel).apply {
        setCellRenderer(this@StockDataBrowser.cellRenderer)
        border = EmptyBorder(5, 5, 5, 5)
        selectionMode = ListSelectionModel.SINGLE_SELECTION
        addListSelectionListener(EquitySelectionListener())
    }

    /**
     * Panel for displaying detailed information about the selected stock
     * Positioned at the bottom of the left panel with fixed dimensions
     */
    val stockDetailsPanel = JPanel(BorderLayout()).apply {
        border = TitledBorder(null, "Stock Details", TitledBorder.LEFT, TitledBorder.TOP)
        preferredSize = Dimension(250, 180)  // Set preferred size to fit in left panel
    }

    /**
     * Form panel using GridBagLayout for organized display of stock details
     */
    val detailsForm = JPanel(GridBagLayout()).apply {
        border = EmptyBorder(10, 10, 10, 10)
    }

    /** Label for stock symbol */
    val symbolLabel = createFormLabel("Symbol:")
    /** Label for company name */
    val nameLabel = createFormLabel("Name:")
    /** Label for stock exchange */
    val exchangeLabel = createFormLabel("Exchange:")
    /** Label for security type */
    val typeLabel = createFormLabel("Type:")
    /** Label for active status */
    val activeLabel = createFormLabel("Active:")

    /** Value field for stock symbol */
    val symbolValue = createFormValue("")
    /** Value field for company name */
    val nameValue = createFormValue("")
    /** Value field for stock exchange */
    val exchangeValue = createFormValue("")
    /** Value field for security type */
    val typeValue = createFormValue("")
    /** Value field for active status */
    val activeValue = createFormValue("")

    /**
     * Table model for displaying OHLCV (Open, High, Low, Close, Volume) technical data
     * Formats date/time values and provides column definitions
     */
    class TechnicalDataModel : AbstractTableModel() {
        /** Column headers for the technical data table */
        private val columnNames = arrayOf("Date", "Open", "High", "Low", "Close", "Volume")
        /** The underlying data collection displayed in the table */
        private var data: List<Array<Any>> = emptyList()
        /** Formatter for converting timestamp values to readable dates */
        private val dateFormat = SimpleDateFormat("yyyy-MM-dd HH:mm")

        /**
         * Updates the table data and refreshes the display
         * @param newData List of data arrays, each representing a row in the table
         */
        fun setData(newData: List<Array<Any>>) {
            data = newData
            fireTableDataChanged()
        }

        /** @return The number of rows in the table */
        override fun getRowCount(): Int = data.size
        /** @return The number of columns in the table */
        override fun getColumnCount(): Int = columnNames.size
        /** @return The name of the specified column */
        override fun getColumnName(column: Int): String = columnNames[column]

        /**
         * Returns the value at the specified cell in the table
         * Handles special formatting for date/time values
         * 
         * @param rowIndex The row index of the cell
         * @param columnIndex The column index of the cell
         * @return The formatted value at the specified cell
         */
        override fun getValueAt(rowIndex: Int, columnIndex: Int): Any {
            if (data.isEmpty() || rowIndex >= data.size) return ""
            val row = data[rowIndex]
            if (columnIndex >= row.size) return ""

            // Handle specific column types
            return when(columnIndex) {
                0 -> if (row[0] is Long) dateFormat.format(Date(row[0] as Long)) else row[0].toString()
                else -> row[columnIndex]
            }
        }
    }

    /** Table model instance for displaying technical data */
    val technicalDataModel = TechnicalDataModel()

    /**
     * Table for displaying technical stock data (OHLCV) with custom rendering
     * Uses right-alignment for numeric columns and left-alignment for date column
     * Row selection triggers candle chart updates
     */
    val technicalDataTable = JTable(technicalDataModel).apply {
        autoResizeMode = JTable.AUTO_RESIZE_ALL_COLUMNS
        fillsViewportHeight = true
        setSelectionMode(ListSelectionModel.SINGLE_SELECTION)

        // Set the renderer for proper alignment of values
        setDefaultRenderer(Object::class.java, object : DefaultTableCellRenderer() {
            override fun getTableCellRendererComponent(
                table: JTable, value: Any?, isSelected: Boolean, hasFocus: Boolean, row: Int, column: Int
            ): Component {
                val c = super.getTableCellRendererComponent(table, value, isSelected, hasFocus, row, column)
                if (column == 0) {
                    horizontalAlignment = JLabel.LEFT
                } else {
                    horizontalAlignment = JLabel.RIGHT
                }
                return c
            }
        })

        // Add selection listener to update the candle chart when a row is selected
        selectionModel.addListSelectionListener { e ->
            if (!e.valueIsAdjusting) {
                val selectedRow = selectedRow
                if (selectedRow >= 0 && selectedEquity != null) {
                    // Get the time value from the selected row (column 0)
                    val timeValue = technicalDataModel.getValueAt(selectedRow, 0)
                    val timestamp = when (timeValue) {
                        is Long -> timeValue
                        is String -> {
                            try {
                                // Try to parse the date string if it's not already a timestamp
                                val format = SimpleDateFormat("yyyy-MM-dd HH:mm")
                                format.parse(timeValue).time
                            } catch (ex: Exception) {
                                System.currentTimeMillis() // Fallback to current time
                            }
                        }
                        else -> System.currentTimeMillis() // Default to current time
                    }

                    // Update the candle chart with the selected time
                    priceChartPanel.updateChart(selectedEquity, timestamp)
                }
            }
        }
    }

    /**
     * Custom panel for displaying candle chart
     * Provides a drawing surface for the CandlePlotter
     */
    class CandleChartPanel : JPanel() {
        private var selectedTime: Long = 0
        private var selectedEquity: Equity? = null
        private val candlePlotter = CandlePlotter()

        init {
            background = Color.WHITE
            border = TitledBorder(null, "Price Chart", TitledBorder.LEFT, TitledBorder.TOP)
        }

        /**
         * Updates the chart with new data and triggers a repaint
         * 
         * @param equity The equity to display data for
         * @param time The end time for the candle chart
         */
        fun updateChart(equity: Equity?, time: Long) {
            selectedEquity = equity
            selectedTime = time
            repaint()
        }

        override fun paintComponent(g: Graphics) {
            super.paintComponent(g)

            if (selectedEquity == null || selectedTime == 0L) {
                return
            }

            val g2d = g.create() as Graphics2D
            try {
                // Setup for high quality rendering
                g2d.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON)

                // Set a clip rect for the plotting area, accounting for the TitledBorder
                val insets = insets

                val clipRect = Rectangle(
                    insets.left ,
                    insets.top ,
                    width - insets.left - insets.right ,
                    height - insets.top - insets.bottom
                )
                g2d.clip = clipRect

                // Pre-transform graphics to map visible space to 100x100 coordinate system
                val scaleX = clipRect.width / 100.0
                val scaleY = clipRect.height / 100.0
                g2d.translate(clipRect.x, clipRect.y + clipRect.height)
                g2d.scale(scaleX, -scaleY)

                // g2d.drawLine(0,0,100,100)
                // Render candles
                candlePlotter.plotCandles(selectedEquity!!, selectedTime, g2d)
            } finally {
                g2d.dispose()
            }
        }
    }

    /**
     * Panel for displaying price charts using CandlePlotter
     */
    val priceChartPanel = CandleChartPanel()


    /**
     * Panel containing date range selector controls with specific start and end date pickers
     * Allows precise filtering of technical data by date range
     */
    val dateRangePanel = JPanel(FlowLayout(FlowLayout.LEFT)).apply {
        add(JLabel("From:"))

        // Start date selector
        val startDateModel = SpinnerDateModel()
        val startDateSpinner = JSpinner(startDateModel).apply {
            // Set the date format in the editor
            editor = JSpinner.DateEditor(this, "yyyy-MM-dd")
            preferredSize = Dimension(120, 25)
            // Set default to 30 days ago
            val cal = Calendar.getInstance()
            cal.add(Calendar.DAY_OF_YEAR, -30)
            value = cal.time
        }
        add(startDateSpinner)

        add(JLabel("To:"))

        // End date selector
        val endDateModel = SpinnerDateModel()
        val endDateSpinner = JSpinner(endDateModel).apply {
            // Set the date format in the editor
            editor = JSpinner.DateEditor(this, "yyyy-MM-dd")
            preferredSize = Dimension(120, 25)
            // Set default to current date
            value = Date()
        }
        add(endDateSpinner)

        // Update button
        val updateButton = JButton("Update").apply {
            addActionListener {
                val startDate = startDateSpinner.value as Date
                val endDate = endDateSpinner.value as Date
                updateTechnicalDataWithRange(selectedEquity, startDate.time, endDate.time)
            }
        }
        add(updateButton)
    }

    /**
     * Panel containing the technical data table and its header controls
     * Displays OHLCV data for the selected stock
     */
    val technicalDataPanel = JPanel(BorderLayout()).apply {
        border = TitledBorder(null, "Technical Data", TitledBorder.LEFT, TitledBorder.TOP)
        add(createTableHeader(), BorderLayout.NORTH)
        add(JScrollPane(technicalDataTable), BorderLayout.CENTER)
    }

    /**
     * Scrollable container for the equity list
     * Provides scrolling capability when the list contains many items
     */
    val stockListScrollPane = JScrollPane(equityList).apply {
        preferredSize = Dimension(250, 300)
        border = EmptyBorder(0, 0, 0, 0)
    }

    /**
     * Panel containing filter controls for the stock list
     * Includes search field and checkbox filters
     */
    val filterPanel = JPanel(BorderLayout()).apply {
        border = TitledBorder(null, "Filter Options", TitledBorder.LEFT, TitledBorder.TOP)
        preferredSize = Dimension(250, 150)

        // Add a search field
        val searchPanel = JPanel(BorderLayout()).apply {
            border = EmptyBorder(10, 10, 10, 10)
            add(JLabel("Search:"), BorderLayout.WEST)
            add(JTextField().apply {
                preferredSize = Dimension(150, 25)
                toolTipText = "Search by symbol or name"
            }, BorderLayout.CENTER)
        }

        // Add filter options
        val filterOptions = JPanel(GridLayout(3, 1, 5, 5)).apply {
            border = EmptyBorder(5, 10, 10, 10)
            add(JCheckBox("Show Active Only").apply { isSelected = true })
            add(JCheckBox("US Exchanges Only").apply { isSelected = true })
            add(JCheckBox("Common Stocks Only").apply { isSelected = true })
        }

        add(searchPanel, BorderLayout.NORTH)
        add(filterOptions, BorderLayout.CENTER)
    }

    /**
     * Main left panel containing filter controls, stock list, and details panel
     * Organized vertically with three sections
     */
    val leftPanel = JPanel(BorderLayout()).apply {
        add(filterPanel, BorderLayout.NORTH)

        // Center panel contains stock list
        add(JPanel(BorderLayout()).apply {
            add(JLabel(" Stock List", JLabel.LEFT), BorderLayout.NORTH)
            add(stockListScrollPane, BorderLayout.CENTER)
        }, BorderLayout.CENTER)

        // Stock details now at the bottom of left panel
        add(stockDetailsPanel, BorderLayout.SOUTH)
    }

    /**
     * Right panel split vertically between price chart and technical data
     * Allows adjusting the relative size of each component
     */
    val rightPanel = JSplitPane(JSplitPane.VERTICAL_SPLIT).apply {
        topComponent = priceChartPanel
        bottomComponent = technicalDataPanel
        resizeWeight = 0.6
        dividerLocation = 400
    }

    /**
     * Main split pane dividing the UI between navigation (left) and content (right)
     * Allows resizing the panels by dragging the divider
     */
    val mainSplit = JSplitPane(JSplitPane.HORIZONTAL_SPLIT).apply {
        leftComponent = leftPanel
        rightComponent = rightPanel
        resizeWeight = 0.25  // Slightly increase the left panel's weight
        dividerLocation = 270  // Give a bit more space to the left panel
    }

    // Track currently selected equity
    private var selectedEquity: Equity? = null

    init {
        // Initialize the UI components
        add(mainSplit, BorderLayout.CENTER)
        setupDetailsPanel()

        // Add tooltip to the candle chart
        priceChartPanel.toolTipText = "Select a row in the data table below to view detailed candle chart"

        // Add tooltip to data table
        technicalDataTable.toolTipText = "Click on a row to view that time period in the candle chart above"

        // Set initial divider locations after component is visible
        SwingUtilities.invokeLater {
            mainSplit.dividerLocation = 250
            rightPanel.dividerLocation = 400
        }
    }

    private fun setupDetailsPanel() {
        // Use smaller insets to make the panel more compact for left panel bottom placement
        val constraints = GridBagConstraints().apply {
            anchor = GridBagConstraints.WEST
            fill = GridBagConstraints.HORIZONTAL
            insets = Insets(3, 3, 3, 3)
        }

        // Add labels
        constraints.gridx = 0
        constraints.gridy = 0
        detailsForm.add(symbolLabel, constraints)

        constraints.gridy = 1
        detailsForm.add(nameLabel, constraints)

        constraints.gridy = 2
        detailsForm.add(exchangeLabel, constraints)

        constraints.gridy = 3
        detailsForm.add(typeLabel, constraints)

        constraints.gridy = 4
        detailsForm.add(activeLabel, constraints)

        // Add values
        constraints.gridx = 1
        constraints.weightx = 1.0

        constraints.gridy = 0
        detailsForm.add(symbolValue, constraints)

        constraints.gridy = 1
        detailsForm.add(nameValue, constraints)

        constraints.gridy = 2
        detailsForm.add(exchangeValue, constraints)

        constraints.gridy = 3
        detailsForm.add(typeValue, constraints)

        constraints.gridy = 4
        detailsForm.add(activeValue, constraints)

        stockDetailsPanel.add(detailsForm, BorderLayout.NORTH)
    }

    /**
     * Creates a standardized label for form field names
     * 
     * @param text The label text to display
     * @return A configured JLabel with consistent styling
     */
    private fun createFormLabel(text: String): JLabel {
        return JLabel(text).apply {
            horizontalAlignment = JLabel.RIGHT
            font = font.deriveFont(Font.BOLD)
            preferredSize = Dimension(80, 20)
        }
    }

    /**
     * Creates a standardized label for displaying form field values
     * 
     * @param initialText The initial text to display (often empty)
     * @return A configured JLabel with consistent styling
     */
    private fun createFormValue(initialText: String): JLabel {
        return JLabel(initialText).apply {
            horizontalAlignment = JLabel.LEFT
            preferredSize = Dimension(160, 20)  // Reduced width to fit in left panel
        }
    }

    /**
     * Creates the header panel for the technical data table
     * Contains date range selector and export button
     * 
     * @return A configured panel with the table header controls
     */
    private fun createTableHeader(): JPanel {
        return JPanel(BorderLayout()).apply {
            add(dateRangePanel, BorderLayout.WEST)

            val actionPanel = JPanel(FlowLayout(FlowLayout.RIGHT))
            val exportButton = JButton("Export").apply {
                addActionListener { exportData() }
            }
            actionPanel.add(exportButton)
            add(actionPanel, BorderLayout.EAST)
        }
    }

    /**
     * Updates the stock details panel with information from the selected equity
     * Clears all fields if no equity is selected
     * 
     * @param equity The selected equity object or null if nothing is selected
     */
    private fun updateEquityDetails(equity: Equity?) {
        if (equity == null) {
            symbolValue.text = ""
            nameValue.text = ""
            exchangeValue.text = ""
            typeValue.text = ""
            activeValue.text = ""
            // Clear candle chart when no equity is selected
            priceChartPanel.updateChart(null, 0)
            return
        }

        symbolValue.text = equity.symbol
        nameValue.text = equity.name
        exchangeValue.text = equity.exchange
        typeValue.text = equity.type
        activeValue.text = equity.active?.toString() ?: "N/A"

        updateTechnicalData(equity)

        // Update candle chart with current time as the end point
        priceChartPanel.updateChart(equity, System.currentTimeMillis())
    }

    /**
     * Updates the technical data table with data from the selected equity
     * Calls updateTechnicalDataWithRange using default date range (all available data)
     * 
     * @param equity The selected equity object or null if nothing is selected
     */
    private fun updateTechnicalData(equity: Equity?) {
        if (equity == null) {
            technicalDataModel.setData(emptyList())
            return
        }

        // Use a wide default range to show all data
        val endTime = System.currentTimeMillis()
        val startTime = endTime - (365L * 24 * 60 * 60 * 1000) // One year ago

        updateTechnicalDataWithRange(equity, startTime, endTime)
    }

    /**
     * Updates the technical data table with data from the selected equity within specified date range
     * Uses efficient direct access to the equity's metrics when possible
     * 
     * @param equity The selected equity object or null if nothing is selected
     * @param startTime Start of the date range in milliseconds since epoch
     * @param endTime End of the date range in milliseconds since epoch
     */
    private fun updateTechnicalDataWithRange(equity: Equity?, startTime: Long, endTime: Long) {
        if (equity == null) {
            technicalDataModel.setData(emptyList())
            return
        }

        // Get the data within the specified date range
        val rangeData = getDataInTimeRange(equity, startTime, endTime)
        technicalDataModel.setData(rangeData)
    }

            /**
     * Efficiently extracts OHLCV data from an equity within the specified time range
     * Uses direct access to year and day indexes for optimal performance
     * 
     * @param equity The equity to extract data from
     * @param startTime Start of the date range in milliseconds since epoch
     * @param endTime End of the date range in milliseconds since epoch
     * @return A list of data arrays representing OHLCV values within the range
     */
    private fun getDataInTimeRange(equity: Equity, startTime: Long, endTime: Long): List<Array<Any>> {
        val result = mutableListOf<Array<Any>>()

        // Get the metrics from the equity
        val metrics = equity.metrics
        if (metrics == null || metrics.isEmpty()) {
            // Fall back to sample data if no metrics are available
            return createSampleDataInRange(equity, startTime, endTime)
        }

        // Get the first metric (e.g., "ohlc_min")
        val metric = metrics.entries.firstOrNull()?.value ?: return createSampleDataInRange(equity, startTime, endTime)

        // Calculate year and day for efficient lookup
        val startCal = Calendar.getInstance()
        val endCal = Calendar.getInstance()
        startCal.timeInMillis = startTime
        endCal.timeInMillis = endTime

        val startYear = startCal.get(Calendar.YEAR)
        val endYear = endCal.get(Calendar.YEAR)

        // Iterate through relevant years only
        for (year in startYear..endYear) {
            val yearData = metric.years[year.toString()] ?: continue

            // Calculate day range for this year
            val startDay = if (year == startYear) startCal.get(Calendar.DAY_OF_YEAR) else 1
            val endDay = if (year == endYear) endCal.get(Calendar.DAY_OF_YEAR) else 366 // Account for leap years

            // Iterate through relevant days only
            for (day in startDay..endDay) {
                val dayData = yearData.days[day.toString()] ?: continue
                val values = dayData.values

                // Filter values within the time range
                for (value in values) {
                    if (value.size >= MetricConstants.Candle.VOLUME.value + 1) {
                        val time = value[MetricConstants.Candle.TIME.value].toLong()

                        // Only include data points within the specified range
                        if (time >= startTime && time <= endTime) {
                            result.add(arrayOf(
                                time,
                                value[MetricConstants.Candle.OPEN.value],
                                value[MetricConstants.Candle.HIGH.value],
                                value[MetricConstants.Candle.LOW.value],
                                value[MetricConstants.Candle.CLOSE.value],
                                value[MetricConstants.Candle.VOLUME.value]
                            ))
                        }
                    }
                }
            }
        }

        // Sort by time for consistent display
        result.sortBy { (it[0] as Number).toLong() }

        return if (result.isNotEmpty()) result else createSampleDataInRange(equity, startTime, endTime)
    }

    /**
             * Creates sample OHLCV data for the selected equity
             * First attempts to extract real data from the equity's metrics
             * Falls back to generating random sample data if no real data is available
             * 
             * @param equity The equity object to create data for
             * @return A list of data arrays representing OHLCV values
             */
    private fun createSampleData(equity: Equity): List<Array<Any>> {
        val result = mutableListOf<Array<Any>>()

        // Try to get real data from the equity's metrics if available
        val metrics = equity.metrics
        if (metrics != null && metrics.isNotEmpty()) {
            // Get the first metric (e.g., "ohlc_min")
            val metric = metrics.entries.firstOrNull()?.value
            if (metric != null) {
                // Get the first year
                val year = metric.years.entries.firstOrNull()?.value
                if (year != null) {
                    // Get all days and their values
                    for (dayEntry in year.days.entries.sortedBy { it.key.toInt() }) {
                        val values = dayEntry.value.values
                        for (value in values) {
                            if (value.size >= MetricConstants.Candle.VOLUME.value + 1) {
                                result.add(arrayOf(
                                    value[MetricConstants.Candle.TIME.value],
                                    value[MetricConstants.Candle.OPEN.value],
                                    value[MetricConstants.Candle.HIGH.value],
                                    value[MetricConstants.Candle.LOW.value],
                                    value[MetricConstants.Candle.CLOSE.value],
                                    value[MetricConstants.Candle.VOLUME.value]
                                ))
                            }
                        }
                    }
                }
            }
        }

        // If we have no real data, create sample data
        if (result.isEmpty()) {
            val currentTime = System.currentTimeMillis()
            val day = 24 * 60 * 60 * 1000L

            // Create 10 days of sample data
            for (i in 0 until 10) {
                val date = currentTime - (i * day)
                val basePrice = 100.0 + (Math.random() * 50.0)
                val open = basePrice
                val high = basePrice + (Math.random() * 5.0)
                val low = basePrice - (Math.random() * 5.0)
                val close = basePrice + (Math.random() * 10.0) - 5.0
                val volume = (Math.random() * 10000.0).toLong() + 1000

                result.add(arrayOf(date, open, high, low, close, volume))
            }
        }

        return result
    }

    /**
     * Creates synthetic sample data within the specified date range
     * Used as a fallback when real market data is not available
     * 
     * @param equity The equity to create sample data for
     * @param startTime Start of the date range in milliseconds since epoch
     * @param endTime End of the date range in milliseconds since epoch
     * @return A list of sample data points within the specified range
     */
    private fun createSampleDataInRange(equity: Equity, startTime: Long, endTime: Long): List<Array<Any>> {
        val result = mutableListOf<Array<Any>>()

        // Calculate how many data points to generate based on the range
        val rangeDuration = endTime - startTime
        val daysInRange = (rangeDuration / (24 * 60 * 60 * 1000)).coerceAtMost(90) // Cap at 90 days

        // Generate evenly spaced data points throughout the range
        val interval = if (daysInRange > 0) rangeDuration / daysInRange else 24 * 60 * 60 * 1000

        // Base price approximately on the equity's symbol hash code for consistency
        val symbolHash = equity.symbol.hashCode()
        val basePrice = 50.0 + (Math.abs(symbolHash % 200))
        var lastClose = basePrice

        // Create data points at regular intervals within the range
        for (i in 0 until daysInRange) {
            val time = startTime + (i * interval)

            // Create realistic price movements based on previous close
            val dailyChange = (Math.random() - 0.5) * 0.03 // -1.5% to +1.5% daily change
            val open = lastClose
            val close = open * (1 + dailyChange)

            // High and low typically extend beyond open/close
            val high = Math.max(open, close) * (1 + Math.random() * 0.01) // Up to 1% above higher of open/close
            val low = Math.min(open, close) * (1 - Math.random() * 0.01) // Up to 1% below lower of open/close

            // Volume tends to correlate with price volatility
            val priceRange = high - low
            val volatilityFactor = priceRange / basePrice * 10
            val volume = (Math.random() * 10000.0 * (1 + volatilityFactor)).toLong() + 1000

            result.add(arrayOf(time, open, high, low, close, volume))

            // Use this close as next open
            lastClose = close
        }

        return result
    }

    /**
     * Handles exporting of technical data
     * Currently shows a placeholder message; would implement actual export functionality
     * in a production version
     */
    private fun exportData() {
        JOptionPane.showMessageDialog(this, "Export functionality would save the current data to CSV or Excel", 
            "Export", JOptionPane.INFORMATION_MESSAGE)
    }

    /**
     * Listener for handling selection events in the equity list
     * Updates the stock details and technical data when a stock is selected
     */
    inner class EquitySelectionListener : ListSelectionListener {
        /**
         * Called when the selection in the equity list changes
         * Updates the UI with details of the newly selected equity
         * 
         * @param e The selection event containing information about the selection change
         */
        override fun valueChanged(e: ListSelectionEvent?) {
            if (!e?.valueIsAdjusting!!) {
                val selectedIndex = equityList.selectedIndex
                if (selectedIndex >= 0) {
                    selectedEquity = equityListModel.getElementAt(selectedIndex)
                    updateEquityDetails(selectedEquity)
                }
            }
        }
    }
}