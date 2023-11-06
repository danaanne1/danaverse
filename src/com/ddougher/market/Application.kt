package com.ddougher.market

import java.awt.BorderLayout
import java.awt.Dimension
import java.awt.event.WindowAdapter
import java.awt.event.WindowEvent
import javax.swing.JDesktopPane
import javax.swing.JFrame
import javax.swing.JMenu
import javax.swing.JMenuBar
import javax.swing.SwingUtilities

class Application  {


    inner class View {
        val desktopPane = JDesktopPane()

        val toolsMenu = JMenu("Tools")

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
    }

    val view = View()

    fun start() {
        SwingUtilities.invokeLater {
            view.mainFrame.isVisible = true
        }
    }

    fun stop() {
        SwingUtilities.invokeLater {
            view.mainFrame.dispose()
        }
    }

}

fun main() {
    Application().start()
}