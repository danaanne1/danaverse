package com.ddougher.market.application

import com.ddougher.market.data.core.Equity
import com.ddougher.market.data.core.Stocks
import com.ddougher.proxamic.ObservableDocumentStore
import java.awt.BorderLayout
import java.awt.Component
import javax.swing.AbstractListModel
import javax.swing.DefaultListCellRenderer
import javax.swing.JLabel
import javax.swing.JList
import javax.swing.JPanel
import javax.swing.JScrollPane
import javax.swing.JSplitPane
import javax.swing.ListModel

/**
 * List of equitites, equity details, table browser for equity technicals by from, to date (or all)
 */
class StockDataBrowser(val stocks: Stocks): JPanel(BorderLayout()) {
    val tickerKeys = stocks.tickers().keys.toSortedSet().toList()
    val equityListModel: AbstractListModel<Equity> = object: AbstractListModel<Equity>() {
        var s = stocks
        override fun getSize(): Int = s.tickers().size
        override fun getElementAt(index: Int): Equity = s.tickers()[tickerKeys[index]]!!
    }
    val cellRenderer = object: DefaultListCellRenderer() {
        override fun getListCellRendererComponent(list: JList<*>?, value: Any?, index: Int, isSelected: Boolean, cellHasFocus: Boolean): Component =
            when (value) {
                is Equity -> super.getListCellRendererComponent(list, "<html>$index <b>${value.symbol}</b> <small>${value.name}</small></html>", index, isSelected, cellHasFocus)
                else -> super.getListCellRendererComponent(list, value, index, isSelected, cellHasFocus)
            }
    }
    val equityList = JList(equityListModel).apply {
        setCellRenderer(this@StockDataBrowser.cellRenderer)
    }

    val topLeft = JScrollPane(equityList)
    val bottomLeft = JScrollPane(JPanel())
    val right = JScrollPane(JPanel())

    val leftSplit = JSplitPane(JSplitPane.VERTICAL_SPLIT, topLeft, bottomLeft)
    val mainSplit = JSplitPane(JSplitPane.HORIZONTAL_SPLIT, leftSplit, JPanel()).also {
        add(BorderLayout.CENTER, it)
    }

}