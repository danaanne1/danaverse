package com.ddougher.market

import java.awt.BorderLayout
import java.awt.Component
import java.awt.Dimension
import java.util.prefs.Preferences
import javax.swing.JPanel
import javax.swing.JScrollPane
import javax.swing.JSplitPane
import javax.swing.JTable
import javax.swing.JTree
import javax.swing.event.*
import javax.swing.table.DefaultTableModel
import javax.swing.table.TableModel
import javax.swing.tree.DefaultTreeCellRenderer
import javax.swing.tree.TreeModel
import javax.swing.tree.TreePath
import javax.swing.tree.TreeSelectionModel

class PreferencesEditor(val prefs: Preferences) : JPanel() {

    val prefsTree = JTree(object: TreeModel {
        val eventListenerList = EventListenerList()
        override fun getRoot(): Any = prefs
        override fun getChild(parent: Any?, index: Int): Any? {
            val p = parent as Preferences
            return p.node(p.childrenNames()[index])
        }
        override fun getChildCount(parent: Any?): Int = (parent as Preferences?)?.childrenNames()?.size ?:0
        override fun isLeaf(node: Any?): Boolean = (node as Preferences?)?.childrenNames()?.size ?:0 <= 0
        override fun getIndexOfChild(parent: Any?, child: Any?): Int = (parent as Preferences).childrenNames().indexOf((child as Preferences).name())
        override fun valueForPathChanged(path: TreePath?, newValue: Any?) {

        }
        override fun addTreeModelListener(l: TreeModelListener?) = listenerList.add(TreeModelListener::class.java, l )
        override fun removeTreeModelListener(l: TreeModelListener?) = listenerList.remove(TreeModelListener::class.java, l)
        private fun fireTreeNodesInserted(e: TreeModelEvent) = fireTreeModelEvent { it.treeNodesInserted(e) }
        private fun fireTreeStructureChange(e: TreeModelEvent) = fireTreeModelEvent { it.treeStructureChanged(e) }
        private fun fireTreeNodesRemoved(e: TreeModelEvent) = fireTreeModelEvent { it.treeNodesRemoved(e) }
        private fun fireTreeNodesChanged(e: TreeModelEvent) = fireTreeModelEvent { it.treeNodesChanged(e) }
        private fun fireTreeModelEvent(handler: (l: TreeModelListener) -> Unit ) {
            val l = listenerList.listenerList
            for (i in l.size-2 downTo 0 step -2 ) {
                if (l[i] != TreeModelListener::class.java) continue
                handler(l[i+1] as TreeModelListener)
            }
        }
    }).apply {
        minimumSize = Dimension(100,100)
        cellRenderer = object: DefaultTreeCellRenderer() {
            override fun getTreeCellRendererComponent(tree: JTree?, value: Any?, sel: Boolean, expanded: Boolean, leaf: Boolean, row: Int, hasFocus: Boolean): Component {
                return super.getTreeCellRendererComponent(tree, (value as Preferences).name(),sel,expanded,leaf,row,hasFocus)
            }
        }
        selectionModel.selectionMode = TreeSelectionModel.SINGLE_TREE_SELECTION
        addTreeSelectionListener { TableModelEvent(prefsTableModel).apply { prefsTableModel.fireTableModelEvent { it.tableChanged(this) }} }
    }

    inner class PrefsTableModel: TableModel {
        override fun getRowCount(): Int = (prefsTree.lastSelectedPathComponent as Preferences?)?.childrenNames()?.size ?: 0
        override fun getColumnCount(): Int = 2
        override fun getColumnName(columnIndex: Int): String = arrayOf("key","value")[columnIndex]
        override fun getColumnClass(columnIndex: Int): Class<*> = String::class.java
        override fun isCellEditable(rowIndex: Int, columnIndex: Int): Boolean = columnIndex == 1
        override fun getValueAt(rowIndex: Int, columnIndex: Int): Any {
            val prefs: Preferences = prefsTree.lastSelectedPathComponent as Preferences? ?: return "null"
            return when (columnIndex) {
                0 -> prefs.childrenNames()[rowIndex]
                else -> prefs.get(prefs.childrenNames()[rowIndex],"")
            }
        }
        override fun setValueAt(aValue: Any?, rowIndex: Int, columnIndex: Int) {
            val prefs: Preferences = prefsTree.lastSelectedPathComponent as Preferences? ?: return
            when (columnIndex) {
                0 -> return
                1 -> prefs.put(prefs.childrenNames()[rowIndex], aValue as String)
            }
            TableModelEvent(this , rowIndex, rowIndex, columnIndex).apply { fireTableModelEvent { it.tableChanged(this) }}
        }
        val eventListenerList = EventListenerList()
        fun fireTableModelEvent(handler: (l: TableModelListener) -> Unit ) {
            val l = listenerList.listenerList
            for (i in l.size-2 downTo 0 step 2 ) {
                if (l[i] != TableModelListener::class.java) continue
                handler(l[i+1] as TableModelListener)
            }
        }
        override fun addTableModelListener(l: TableModelListener?) = eventListenerList.add(TableModelListener::class.java, l)
        override fun removeTableModelListener(l: TableModelListener?) = eventListenerList.remove(TableModelListener::class.java, l)
    }
    val prefsTableModel = PrefsTableModel()
    val prefsTable: JTable = JTable(prefsTableModel)

    val rightScrillPane = JScrollPane(prefsTable)
    val leftScollPane = JScrollPane(prefsTree)
    val splitPane = JSplitPane(JSplitPane.HORIZONTAL_SPLIT, leftScollPane, rightScrillPane)

    init {
        layout = BorderLayout()
        add(splitPane, BorderLayout.CENTER)
    }

}