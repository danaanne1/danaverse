package com.ddougher.market

import com.ddougher.proxamic.MemoryMappedDocumentStore
import com.ddougher.proxamic.ObservableDocumentStore
import com.theunknowablebits.proxamic.DocumentStore
import java.awt.BorderLayout
import java.awt.Dimension
import java.awt.event.ActionEvent
import java.awt.event.ComponentAdapter
import java.awt.event.ContainerAdapter
import java.awt.event.WindowAdapter
import java.awt.event.WindowEvent
import java.awt.event.WindowStateListener
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.ByteArrayInputStream
import java.io.File
import java.io.FileInputStream
import java.io.IOException
import java.io.ObjectInput
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.util.*
import java.util.prefs.Preferences
import javax.swing.*

class Application  {

    val preferences = Preferences.userNodeForPackage(javaClass).apply {
        node("DocStore").apply { put(com.ddougher.market.Constants.DOC_STORE_BASE_PATH_KEY, get(com.ddougher.market.Constants.DOC_STORE_BASE_PATH_KEY, com.ddougher.market.Constants.DOC_STORE_DEFAULT_FOLDER_NAME)) }
    }


    val docStore: MemoryMappedDocumentStore =
            preferences.node("DocStore").get(Constants.DOC_STORE_BASE_PATH_KEY, Constants.DOC_STORE_DEFAULT_FOLDER_NAME).let { path ->
                @Suppress("ComplexRedundantLet")
                File(path).apply { mkdirs() }.let { File(it, "Database.dt1") }.let { file ->
                    if (file.exists())
                        ObjectInputStream(BufferedInputStream(file.inputStream(), 65536)).use { it.readObject() as MemoryMappedDocumentStore }
                    else
                        MemoryMappedDocumentStore(Optional.of(path), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty())
                }
            }

    inner class View {

        val desktopPane = JDesktopPane()


        val toolsMenu: JMenu = JMenu("File").apply {
            add(Utils.actionFu("Preferences") {
                preferencesDialog.isVisible = true
            })
        }

        val mainMenuBar = JMenuBar().apply {
            add(toolsMenu)
        }

        val mainFrame = JFrame("Dana Trade 2.0").apply {
            JFrame.setDefaultLookAndFeelDecorated(true)
            defaultCloseOperation = JFrame.DO_NOTHING_ON_CLOSE
            preferredSize = Dimension(1500, 500)
            addWindowListener(object : WindowAdapter() {
                override fun windowClosing(e: WindowEvent) {
                    println("Frame Closing")
                    this@Application.stop()
                }

                override fun windowClosed(e: WindowEvent) {
                    println("Frame closed")
                }

                override fun windowActivated(e: WindowEvent) {
                    println("Frame activated")
                }

                override fun windowDeactivated(e: WindowEvent) {
                    println("Frame deactivated")
                }
            })
            rootPane.contentPane.add(BorderLayout.CENTER, desktopPane)
            rootPane.jMenuBar = mainMenuBar
            pack()
        }

        val preferencesDialog = JDialog(mainFrame, "Preferences", false).apply {
            layout = BorderLayout()
            add(PreferencesEditor(preferences), BorderLayout.CENTER)
        }

    }
    val view = View()

    fun start() {
        SwingUtilities.invokeLater {
            view.mainFrame.isVisible = true
        }
    }

    fun stop() {
        docStore.close()
        preferences.node("DocStore").get(Constants.DOC_STORE_BASE_PATH_KEY, Constants.DOC_STORE_DEFAULT_FOLDER_NAME).also { path ->
            @Suppress("ComplexRedundantLet")
            File(path).apply { mkdirs() }.let { File(it, "Database.dt1") }.apply {
                ObjectOutputStream(BufferedOutputStream(outputStream(), 65536)).use { it.writeObject(docStore) }
            }
        }
        println("Docstore closed")
        SwingUtilities.invokeLater {
            view.mainFrame.dispose()
        }
    }

}

fun main() {
    UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
    Application().start()
}