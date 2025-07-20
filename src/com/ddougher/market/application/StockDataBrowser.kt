package com.ddougher.market.application

import com.ddougher.market.data.MetricConstants
import com.ddougher.market.data.core.Equity
import com.ddougher.market.data.core.Stocks
import com.ddougher.proxamic.ObservableDocumentStore
import java.awt.*
import java.text.SimpleDateFormat
import java.util.Date
import javax.swing.*
import javax.swing.border.EmptyBorder
import javax.swing.border.TitledBorder
import javax.swing.event.ListSelectionEvent
import javax.swing.event.ListSelectionListener
import javax.swing.table.AbstractTableModel
import javax.swing.table.DefaultTableCellRenderer
import javax.swing.table.JTableHeader

/**
 * Stock data browser implementing the UI design for viewing and analyzing equity data
 */
class StockDataBrowser(val stocks: Stocks): JPanel(BorderLayout()) {
    // Equity list model and components
    val tickerKeys = stocks.tickers().keys.toSortedSet().toList()
    val equityListModel: AbstractListModel<Equity> = object: AbstractListModel<Equity>() {
        var s = stocks
        override fun getSize(): Int = s.tickers().size
        override fun getElementAt(index: Int): Equity = s.tickers()[tickerKeys[index]]!!
    }

    val cellRenderer = object: DefaultListCellRenderer() {
        override fun getListCellRendererComponent(list: JList<*>?, value: Any?, index: Int, isSelected: Boolean, cellHasFocus: Boolean): Component =
            when (value) {
                is Equity -> super.getListCellRendererComponent(list, "<html><b>${value.symbol}</b> <small>${value.name}</small></html>", index, isSelected, cellHasFocus)
                else -> super.getListCellRendererComponent(list, value, index, isSelected, cellHasFocus)
            }
    }

    val equityList = JList(equityListModel).apply {
        setCellRenderer(this@StockDataBrowser.cellRenderer)
        border = EmptyBorder(5, 5, 5, 5)
        selectionMode = ListSelectionModel.SINGLE_SELECTION
        addListSelectionListener(EquitySelectionListener())
    }

    // Stock details panel - more compact for left panel placement
    val stockDetailsPanel = JPanel(BorderLayout()).apply {
        border = TitledBorder(null, "Stock Details", TitledBorder.LEFT, TitledBorder.TOP)
        preferredSize = Dimension(250, 180)  // Set preferred size to fit in left panel
    }

    // Create the form-like layout for stock details
    val detailsForm = JPanel(GridBagLayout()).apply {
        border = EmptyBorder(10, 10, 10, 10)
    }

    val symbolLabel = createFormLabel("Symbol:")
    val nameLabel = createFormLabel("Name:")
    val exchangeLabel = createFormLabel("Exchange:")
    val typeLabel = createFormLabel("Type:")
    val activeLabel = createFormLabel("Active:")

    val symbolValue = createFormValue("")
    val nameValue = createFormValue("")
    val exchangeValue = createFormValue("")
    val typeValue = createFormValue("")
    val activeValue = createFormValue("")

    // Technical data table model defined as a class
    class TechnicalDataModel : AbstractTableModel() {
        private val columnNames = arrayOf("Date", "Open", "High", "Low", "Close", "Volume")
        private var data: List<Array<Any>> = emptyList()
        private val dateFormat = SimpleDateFormat("yyyy-MM-dd HH:mm")

        fun setData(newData: List<Array<Any>>) {
            data = newData
            fireTableDataChanged()
        }

        override fun getRowCount(): Int = data.size
        override fun getColumnCount(): Int = columnNames.size
        override fun getColumnName(column: Int): String = columnNames[column]

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

    // Instance of the technical data model
    val technicalDataModel = TechnicalDataModel()

    // Table for technical data with custom renderer
    val technicalDataTable = JTable(technicalDataModel).apply {
        autoResizeMode = JTable.AUTO_RESIZE_ALL_COLUMNS
        fillsViewportHeight = true
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
    }

    // Price chart panel (placeholder)
    val priceChartPanel = JPanel().apply {
        border = TitledBorder(null, "Price Chart", TitledBorder.LEFT, TitledBorder.TOP)
        background = Color.WHITE
    }


    // Date range selector
    val dateRangePanel = JPanel(FlowLayout(FlowLayout.LEFT)).apply {
        add(JLabel("Date Range:"))
        val dateRanges = arrayOf("All", "1 Week", "1 Month", "3 Months", "6 Months", "1 Year", "5 Years")
        val comboBox = JComboBox(dateRanges).apply {
            selectedIndex = 0
            preferredSize = Dimension(120, 25)
            addActionListener { updateTechnicalData(selectedEquity) }
        }
        add(comboBox)
    }

    // Technical data panel
    val technicalDataPanel = JPanel(BorderLayout()).apply {
        border = TitledBorder(null, "Technical Data", TitledBorder.LEFT, TitledBorder.TOP)
        add(createTableHeader(), BorderLayout.NORTH)
        add(JScrollPane(technicalDataTable), BorderLayout.CENTER)
    }

    // Initialize the layout
    val stockListScrollPane = JScrollPane(equityList).apply {
        preferredSize = Dimension(250, 300)
        border = EmptyBorder(0, 0, 0, 0)
    }

    // Create a placeholder panel for the top of the left panel
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

    // Left panel with filter at top, stock list in the middle, and details at the bottom
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

    // Price chart is now the top component in the right panel
    val rightPanel = JSplitPane(JSplitPane.VERTICAL_SPLIT).apply {
        topComponent = priceChartPanel
        bottomComponent = technicalDataPanel
        resizeWeight = 0.6
        dividerLocation = 400
    }

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

    private fun createFormLabel(text: String): JLabel {
        return JLabel(text).apply {
            horizontalAlignment = JLabel.RIGHT
            font = font.deriveFont(Font.BOLD)
            preferredSize = Dimension(80, 20)
        }
    }

    private fun createFormValue(initialText: String): JLabel {
        return JLabel(initialText).apply {
            horizontalAlignment = JLabel.LEFT
            preferredSize = Dimension(160, 20)  // Reduced width to fit in left panel
        }
    }

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

    private fun updateEquityDetails(equity: Equity?) {
        if (equity == null) {
            symbolValue.text = ""
            nameValue.text = ""
            exchangeValue.text = ""
            typeValue.text = ""
            activeValue.text = ""
            return
        }

        symbolValue.text = equity.symbol
        nameValue.text = equity.name
        exchangeValue.text = equity.exchange
        typeValue.text = equity.type
        activeValue.text = equity.active?.toString() ?: "N/A"

        updateTechnicalData(equity)
    }

    private fun updateTechnicalData(equity: Equity?) {
        // This would normally load data from the equity's metrics
        if (equity == null) {
            technicalDataModel.setData(emptyList())
            return
        }

        // For demonstration, create sample data
        // In a real implementation, this would access equity.getMetrics()
        val sampleData = createSampleData(equity)
        technicalDataModel.setData(sampleData)
    }

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

    private fun exportData() {
        JOptionPane.showMessageDialog(this, "Export functionality would save the current data to CSV or Excel", 
            "Export", JOptionPane.INFORMATION_MESSAGE)
    }

    inner class EquitySelectionListener : ListSelectionListener {
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