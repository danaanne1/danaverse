package com.ddougher.market

import java.awt.BorderLayout
import java.awt.Component
import java.awt.Dimension
import java.util.prefs.Preferences
import javax.swing.JPanel
import javax.swing.JScrollPane
import javax.swing.JSplitPane
import javax.swing.JTree
import javax.swing.event.EventListenerList
import javax.swing.event.TreeModelEvent
import javax.swing.event.TreeModelListener
import javax.swing.tree.DefaultTreeCellRenderer
import javax.swing.tree.TreeModel
import javax.swing.tree.TreePath

class PreferencesEditor(val prefs: Preferences) : JPanel() {

    val prefsTree = JTree(object: TreeModel {
        val eventListenerList = EventListenerList()

        override fun getRoot(): Any {
            return prefs
        }

        override fun getChild(parent: Any?, index: Int): Any? {
            val p = parent as Preferences
            return p.node(p.childrenNames()[index])
        }

        override fun getChildCount(parent: Any?): Int {
            return (parent as Preferences).childrenNames().size
        }

        override fun isLeaf(node: Any?): Boolean {
            return (node as Preferences).childrenNames().size > 0
        }

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
    }

    val rightScrillPane = JScrollPane(JPanel())

    val leftScollPane = JScrollPane(prefsTree)

    val splitPane = JSplitPane(JSplitPane.HORIZONTAL_SPLIT, leftScollPane, rightScrillPane)

    init {
        layout = BorderLayout()
        add(splitPane, BorderLayout.CENTER)
    }

}