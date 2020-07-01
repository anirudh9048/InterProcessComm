package com.company;
import java.net.*;
import java.io.*;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;


public class App2 {
    static final int MESSAGE_SIZE = 200;
    static ServerSocket serverSocket;
    static volatile Socket clientSocketA; // for handling the clients that connected to this server
    static Socket clientSocketB; // for connecting to other servers
    static final int serverPortNum = 201;

    static byte[] destination = new byte[MESSAGE_SIZE];
    static OutputStream writer;
    static InputStream reader;

    static Queue<String> msgQueue = new ConcurrentLinkedQueue<>();

    static boolean master = false;
    static String role = null;

    public static void main(String[] args) {
        System.out.println("Starting App 2 ... ");

        initConnections();
        initApp();

        if (master) {
            // assign map to worker 2 and reduce to worker 3
            connectTo(201);
            writeMessage("ASSIGN:MAP");
            connectTo(202);
            writeMessage("ASSIGN:REDUCE");
        } else {
            // await assignment
            while (role == null) {Thread.onSpinWait();}
            if (role.equals("MAP")) {
                System.out.println("mapper");
            } else if (role.equals("REDUCE")) {
                System.out.println("reducer");
            }
        }

    }

    public static void parseAndSave(String command) {
        String[] res = command.split(":");
        switch(res[0]) {
            case "ASSIGN":
                if (res.length > 1) {
                    if (res[1].equals("MAP") || res[1].equals("REDUCE")) {
                        role = res[1];
                    }
                }
                break;
            default:
                System.out.println("ERR: Unknown command");
        }
    }

    public static void initApp() {
        (new Thread() {
            @Override
            public void run() {
                while (true) {
                    waitForConnection();
                }
            }
        }).start();

        (new Thread() {
            @Override
            public void run() {
                while (true) {
                    String s = read();
                    msgQueue.add(s);
                }
            }
        }).start();

        (new Thread() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    String command = msgQueue.poll();
                    if (command != null) {
                        // process the command
                        System.out.println("Reader thread received command: " + command);
                        parseAndSave(command);
                    }
                }
            }
        }).start();
    }

    public static synchronized String read() {
        // don't read on a null socket !
        while (clientSocketA == null) Thread.onSpinWait();
        try {
            reader = clientSocketA.getInputStream();
            int result = reader.read(destination, 0, MESSAGE_SIZE);
            return new String(destination, 0, result);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "ERROR";
    }

    public static void writeMessage(String message) {
        try {
            writer = clientSocketB.getOutputStream();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            writer.write(message.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void initConnections() {
        try {
            serverSocket = new ServerSocket(serverPortNum);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("App2: Started server");
    }

    public static void waitForConnection() {
        try {
            clientSocketA = serverSocket.accept();
            System.out.println("Connected with client");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void connectTo(int destPort) {
        while (true) {
            try {
                clientSocketB = new Socket("127.0.0.1", destPort);
                break;
            } catch (Exception e) {
                e.printStackTrace();
                continue;
            }
        }
    }
}
