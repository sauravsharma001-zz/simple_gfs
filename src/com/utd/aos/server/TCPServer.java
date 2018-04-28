/**
 *
 * TCPServer.java - Server code, respond to different operation requested by a Client like Read, Write and Create
 * @author  Saurav Sharma and Amal Roy
 *
 */

package com.utd.aos.server;

import com.utd.aos.util.CreateFile;
import com.utd.aos.util.GetFolderDetails;
import com.utd.aos.util.ReadFile;
import com.utd.aos.util.WriteFile;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TCPServer extends TimerTask{

    private ServerSocket serverSocket;
    private static int port;
    private String directoryPath;
    private static Map ClientList;
    private static String MetaDataServer;
    private int noOfClients;
    private final String ServerID;

    public TCPServer(Properties properties, String serverID) throws IOException {

        this.port = Integer.valueOf(properties.getProperty("serverport"));
        this.serverSocket = new ServerSocket(port);
        this.directoryPath = properties.getProperty("directoryserver" + serverID);
        ClientList = new LinkedHashMap();
        this.noOfClients = 0;
        this.ServerID = serverID;
        this.MetaDataServer = properties.getProperty("medataserver");
    }

    /**
     * Start the server to start listening from clients
     */
    public void startServer()   {

        System.out.println("Starting Server " + ServerID);
        System.out.println("-------------------------------------------------------");
        System.out.println("Waiting for client on port " +
                serverSocket.getLocalPort() + "...");
        System.out.println();
        ExecutorService pool = Executors.newFixedThreadPool(2);
        while(true) {

            try {
                Socket server = serverSocket.accept();
                String clientIP = server.getInetAddress().toString().substring(1);
                DataInputStream in = new DataInputStream(server.getInputStream());
                DataOutputStream out = new DataOutputStream(server.getOutputStream());

                String msg = in.readUTF();
                System.out.println("message received: " + msg);

                int clientID = Integer.parseInt(msg.substring(msg.indexOf("Client ID: ") + 11, msg.indexOf("Client ID: ") + 12).trim());

                if (!ClientList.containsKey(clientID)) {
                    ClientList.put(clientID, clientIP);
                }

                // To handle READ request from Clients
                if (msg.contains("READ: ")) {
                    String fileName = msg.split(";")[1].split(":")[1].trim();
                    int startOffset =  Integer.valueOf(msg.split(";")[2].split(":")[1].trim());
                    int endOffset =  Integer.valueOf(msg.split(";")[3].split(":")[1].trim());
                    Callable<String> readFileCallable = new ReadFile(directoryPath, fileName, startOffset, endOffset);
                    Future<String> futureRead = pool.submit(readFileCallable);
                    String content = futureRead.get();
                    out.writeUTF(content);
                }
                // To handle WRITE request from Clients
                else if (msg.contains("WRITE: ")) {
                    String fileName = msg.split(";")[1].split(":")[1].trim();
                    Callable<Boolean> writeFileCallable = new WriteFile(directoryPath, fileName,
                            msg.split(";")[2].split(":")[1].trim());
                    Future<Boolean> futureWrite = pool.submit(writeFileCallable);
                    Boolean flag = futureWrite.get();
                    if (flag) {
                        out.writeUTF("File Successfully Updated");
                    } else {
                        out.writeUTF("File Not Updated");
                    }
                }
                // To handle FILE CREATE request from Metadata Server
                else if (msg.contains("CREATE: ")) {
                    String fileName = msg.split(";")[1].split(":")[1].trim();
                    if(msg.split(";").length == 3)  {
                        fileName = fileName + "_00" + msg.split(";")[2].split(":")[1].trim() + ".txt";
                    }
                    else    {
                        fileName = fileName + "_001.txt";
                    }
                    Callable<String> createFileCallable = new CreateFile(directoryPath, fileName);
                    Future<String> futureCreate = pool.submit(createFileCallable);
                    String content = futureCreate.get();
                    out.writeUTF(content);
                }
                else    {
                    out.writeUTF("Operation not supported");
                }

            } catch (SocketTimeoutException s) {
                System.out.println("Socket timed out!");
                break;
            } catch (IOException e) {
                e.printStackTrace();
                break;
            } catch (Exception ex)  {
                ex.printStackTrace();
                break;
            }
        }
        pool.shutdown();
    }

    public void run() {

        try {

            Socket metadataServer = new Socket(MetaDataServer, port);
            DataInputStream in = new DataInputStream(metadataServer.getInputStream());
            DataOutputStream out = new DataOutputStream(metadataServer.getOutputStream());
            ExecutorService pool = Executors.newFixedThreadPool(2);
            Callable<String> getFolderDetailsCallable = new GetFolderDetails(directoryPath);
            Future<String> futureGetDetails = pool.submit(getFolderDetailsCallable);
            String content = futureGetDetails.get();
            out.writeUTF("HEARTBEAT: " + ServerID + ";" + content);
//            String msg = in.readUTF();
//            System.out.println(msg);
            pool.shutdown();
            in.close();
            out.close();
            metadataServer.close();
        } catch (Exception ex) {
            System.out.println("Metadata Server is down ");
        }
    }
}
