/**
 *
 * TCPClient.java - Client code, Make different request to Server like reading, writing or creating a file
 * @author: Saurav Sharma and Amal Roy
 *
 */

package com.utd.aos.client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class TCPClient {


    class ClientInfo {
        private volatile List<String> serverAddress = new ArrayList<>();
        private volatile String metadataServerAddress;
        private volatile int serverPort, clientPort;
        private volatile Socket[] clients;
        private volatile int localClock = 1;
        private volatile int clientID;
        Random rand = new Random();
        private volatile int noOfRequests;
        private volatile ArrayList<String> typeOfOperations;
        volatile Boolean isRunning = true;
    }

    final ClientInfo clientInfo = new ClientInfo();
    private int noOfFile;
    private int chunkSize;

    public TCPClient(Properties properties, String clientID) {


        for (int i = 1; i <= Integer.valueOf(properties.getProperty("noofserver")); i++)
            this.clientInfo.serverAddress.add(properties.getProperty("server" + i));

        System.out.println("");
        this.clientInfo.metadataServerAddress = properties.getProperty("medataserver");
        this.clientInfo.serverPort = Integer.valueOf(properties.getProperty("serverport"));
        this.clientInfo.clientPort = Integer.valueOf(properties.getProperty("clientport"));
        this.clientInfo.noOfRequests = Integer.valueOf(properties.getProperty("noofrequests"));
        this.clientInfo.typeOfOperations = new ArrayList<>();
        this.clientInfo.typeOfOperations.add("READ: ");
        this.clientInfo.typeOfOperations.add("WRITE: ");
        this.clientInfo.typeOfOperations.add("CREATE: ");
        this.clientInfo.clientID = Integer.parseInt(clientID);
        this.noOfFile = Integer.valueOf(properties.getProperty("nooffiles"));
        this.chunkSize = Integer.valueOf(properties.getProperty("chunksize"));
    }

    public void startClient() {
        try {

            System.out.println("Starting Client " + clientInfo.clientID);
            int serverID = 0, operationID, fileID, i = 0;
            Socket[] serverSockets = new Socket[clientInfo.serverAddress.size()];
            Thread.sleep(5000);
            while (i < clientInfo.noOfRequests) {

                Socket metadataServerSocket = new Socket(clientInfo.metadataServerAddress, clientInfo.serverPort);
                i++;
                System.out.println();
                System.out.println("---------------------------------------------------");
                System.out.println("Request No: " + i);
                operationID = 0;
//                operationID = clientInfo.rand.nextInt(clientInfo.typeOfOperations.size());
                fileID = clientInfo.rand.nextInt(noOfFile) + 1;
                if(operationID < 1)   {

                    DataInputStream in = new DataInputStream(metadataServerSocket.getInputStream());
                    DataOutputStream out = new DataOutputStream(metadataServerSocket.getOutputStream());

                    if (operationID == 0) {
                        int startOffset = clientInfo.rand.nextInt(21);
                        int endOffset = clientInfo.rand.nextInt(21) + startOffset;
                        int sof, eof;
                        System.out.println("Operation " + clientInfo.typeOfOperations.get(operationID).substring(0, clientInfo.typeOfOperations.get(operationID).length() - 2) + " " + fileID  + "; StartOffset: " + startOffset + "; EndOffset: " + endOffset + ";");
                        out.writeUTF("Client ID: " + clientInfo.clientID + "; " + clientInfo.typeOfOperations.get(operationID) + fileID + "; StartOffset: " + startOffset + "; EndOffset: " + endOffset + ";");
                        String msgFromMetadata = in.readUTF();
                        System.out.println("Message Received from Metadata Server: " + msgFromMetadata);
                        if (msgFromMetadata.contains(",")) {
                            StringBuilder fileContent = new StringBuilder();
                            String[] chunkDetail = msgFromMetadata.split(";");
                            for(int c = 0; c < chunkDetail.length; c++) {
                                String chunk = chunkDetail[c];
                                if(c == 0)
                                    sof = startOffset % chunkSize;
                                else
                                    sof = -1;

                                if(c == chunkDetail.length - 1)
                                    eof = endOffset % chunkSize;
                                else
                                    eof = -1;

                                if(chunk.contains("Not Available")) {
                                    fileContent.append("Unable to read chunk, Server is Down");
                                }
                                else if(chunk != null && chunk.length() > 0){
//                                    System.out.println("chunk " + chunk);
                                    int sId = Integer.valueOf(chunk.split(",")[1].trim());
                                    String chunkName = chunk.split(",")[0];
                                    Socket fileServer = new Socket(clientInfo.serverAddress.get(sId - 1), clientInfo.serverPort);
                                    DataInputStream infileServer = new DataInputStream(fileServer.getInputStream());
                                    DataOutputStream outfileServer = new DataOutputStream(fileServer.getOutputStream());
                                    outfileServer.writeUTF("Client ID: " + clientInfo.clientID + "; " + "READ: " + fileID + "_" + chunkName + "; StartOffset: " + sof + "; EndOffset: " + eof + ";");
                                    String msgFromFileServer = infileServer.readUTF();
                                    fileContent.append(msgFromFileServer);
                                    fileContent.append(" ");
                                    fileServer.close();
                                }
                            }
                            System.out.println("Chunk Content: " + fileContent);
                        }
                        else    {
                            System.out.println(msgFromMetadata);
                        }
                    } else  {
                        System.out.println("Operation " + clientInfo.typeOfOperations.get(operationID).substring(0, clientInfo.typeOfOperations.get(operationID).length() - 2) + " " + fileID);
                        String content = "New line Added";
                        out.writeUTF("Client ID: " + clientInfo.clientID + "; " + clientInfo.typeOfOperations.get(operationID) + fileID + "; Content: " + content + ";");
                        String msgFromMetadata = in.readUTF();
                        System.out.println("Message Received from Metadata Server: " + msgFromMetadata);
                        if(msgFromMetadata!= null && !msgFromMetadata.contains("Not Available") && msgFromMetadata.contains(",") && msgFromMetadata.contains(";")) {
                            int sId = Integer.valueOf(msgFromMetadata.split(";")[0].split(",")[1].trim());
                            Socket fileServer = new Socket(clientInfo.serverAddress.get(sId - 1), clientInfo.serverPort);
                            DataInputStream infileServer = new DataInputStream(fileServer.getInputStream());
                            DataOutputStream outfileServer = new DataOutputStream(fileServer.getOutputStream());
                            outfileServer.writeUTF("Client ID: " + clientInfo.clientID + "; " + clientInfo.typeOfOperations.get(operationID) + fileID + "_" + msgFromMetadata.split(";")[0].split(",")[0] + "; Content: " + content + ";");
                            String msgFromFileServer = infileServer.readUTF();
                            System.out.println("Response: " + msgFromFileServer);
                            fileServer.close();
                        }
                    }
                }
                else    {
                    DataInputStream in = new DataInputStream(metadataServerSocket.getInputStream());
                    DataOutputStream out = new DataOutputStream(metadataServerSocket.getOutputStream());
                    noOfFile += 1;
                    System.out.println("Operation " + clientInfo.typeOfOperations.get(operationID).substring(0, clientInfo.typeOfOperations.get(operationID).length() - 2) + "  " + noOfFile);
                    out.writeUTF("Client ID: " + clientInfo.clientID + "; " + clientInfo.typeOfOperations.get(operationID) + noOfFile + ";");
                    String msgFromMetadata = in.readUTF();
                    System.out.println("Message Received from Metadata Server: " + msgFromMetadata);
//                    Socket fileServer = new Socket(clientInfo.serverAddress.get(sId-1), clientInfo.serverPort);
//                    DataInputStream infileServer = new DataInputStream(fileServer.getInputStream());
//                    DataOutputStream outfileServer = new DataOutputStream(fileServer.getOutputStream());
//                    outfileServer.writeUTF("Client ID: " + clientInfo.clientID + "; " + "CREATE: " + fileID++ + ";");
                }

//                System.out.println("Request Over");
                System.out.println("---------------------------------------------------");
                metadataServerSocket.close();
                Thread.sleep(3000);
            }
            System.out.println();
            Thread.sleep(15000);
        } catch (IOException ioEx) {
            ioEx.printStackTrace();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}