package com.ddougher.market;

import javax.swing.*;
import java.awt.*;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;

public class Application {

    public static final Application instance = new Application();
    View view = new View();



    class View {

        JDesktopPane desktopPane;
        JFrame mainFrame;

        public JFrame mainFrame() {
            if (mainFrame!=null)
                return mainFrame;
            JFrame.setDefaultLookAndFeelDecorated(true);
            mainFrame = new JFrame("Dana Trade 1.0");
            mainFrame.setDefaultCloseOperation(JFrame.DO_NOTHING_ON_CLOSE);
            mainFrame.setPreferredSize(new Dimension(1500,500));
            mainFrame.addWindowListener(new WindowAdapter() {
                @Override
                public void windowClosing(WindowEvent e) {
                    System.out.println("Frame Closing");
                    Application.this.stop();
                }
                @Override public void windowClosed(WindowEvent e) { System.out.println("Frame closed"); }
                @Override public void windowActivated(WindowEvent e) { System.out.println("Frame activated"); }
                @Override public void windowDeactivated(WindowEvent e) { System.out.println("Frame deactivated"); }
            });
            mainFrame.getRootPane().getContentPane().add(BorderLayout.CENTER, desktopPane());
            mainFrame.getRootPane().setJMenuBar(mainMenuBar());
            mainFrame.pack();
            return mainFrame;
        }

        private JDesktopPane desktopPane() {
            desktopPane = new JDesktopPane();
            return desktopPane;
        }

        private JMenuBar mainMenuBar() {
            JMenuBar menuBar = new JMenuBar();
            menuBar.add(toolsMenu());
            return menuBar;
        }

        private JMenu toolsMenu() {

            JMenu tools = new JMenu("Tools", false);
            return tools;
        }


    }



    void start() {
        view.mainFrame().setVisible(true);
    }


    void stop() {
        view.mainFrame.dispose();
    }

    public static void main(String [] args) {
        Application.instance.start();
    }
}
