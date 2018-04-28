/**
 *
 * TCPMetadataServer.java - Metadata Server, stores
 * @author  Saurav Sharma and Amal Roy
 *
 */

package com.utd.aos.metadata;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TCPMetadataServer extends TimerTask {

    class ClientInfo  {
        private int id;
        private boolean alive;
        private String typeOfClient;
        private String address;
        private Instant lastUpdatedTimeStamp;
        private Map<String, Map> fileDetail;

        public ClientInfo(int id, String typeOfClient, String address) {
            this.id = id;
            this.alive = false;
            this.typeOfClient = typeOfClient;
            this.address = address;
            this.lastUpdatedTimeStamp = null;
            this.fileDetail = new LinkedHashMap<>();
        }
    }

    private static int port;
    private ServerSocket serverSocket;
    private final Map<Integer, ClientInfo> ClientList;
    private Set<Integer> activeServer = new HashSet<>();
    private Map<String, SortedSet<String>> fileLocationDetail;
    Random rand = new Random();
    private int chunkSize;

    public TCPMetadataServer(Properties properties) throws IOException  {

        this.port = Integer.valueOf(properties.getProperty("serverport"));
        this.serverSocket = new ServerSocket(Integer.valueOf(properties.getProperty("serverport")));
        this.ClientList = new LinkedHashMap();
        this.fileLocationDetail = new LinkedHashMap<>();
        this.chunkSize = Integer.valueOf(properties.getProperty("chunksize"));
        for(int i = 1; i <= Integer.valueOf(properties.getProperty("noofserver")); i++) {
            String add = properties.getProperty("server" + i);
            ClientInfo cInfo = new ClientInfo(i, "server", add);
            this.ClientList.put(i, cInfo);
        }
    }

    /**
     * Start the server to start listening from clients
     */
    public void startServer() {

        System.out.println("Starting Metadata Server ");
        System.out.println("-------------------------------------------------------");
        System.out.println("Waiting on port " +
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

                if(msg.contains("HEARTBEAT: ")) {

                    int id = Integer.valueOf(msg.substring(msg.indexOf(";")-1, msg.indexOf(";")));
                    if(ClientList.containsKey(id)) {
                        activeServer.add(id);
                        ClientInfo serverName = ClientList.get(id);
                        serverName.alive = true;
                        msg = msg.substring(msg.indexOf(";") + 1);
                        String[] filesDetail = msg.split(";");
                        for (String fileDetail : filesDetail) {
                            String[] fileInfo = fileDetail.split(",");
                            String fileName = fileInfo[0].split("_")[0];
                            if(!fileLocationDetail.containsKey(fileName))   {
                                SortedSet chunkList = new TreeSet();
                                chunkList.add(fileInfo[0].split("_")[1] + "," + id);
                                fileLocationDetail.put(fileInfo[0].split("_")[0], chunkList);
                            }
                            else    {
                                SortedSet chunkList = fileLocationDetail.get(fileName);
                                chunkList.add(fileInfo[0].split("_")[1] + "," + id);
                                fileLocationDetail.put(fileName, chunkList);
                            }
                            if(serverName.fileDetail.containsKey(fileName)) {
                                Map fileMetadata = serverName.fileDetail.get(fileName);
                                fileMetadata.put(fileInfo[0].split("_")[1], fileDetail.substring(fileDetail.indexOf(";")+1));
                                serverName.lastUpdatedTimeStamp = Instant.now();
                                serverName.fileDetail.put(fileName, fileMetadata);
                            }
                            else    {
                                Map fileMetadata = new LinkedHashMap();
//                                fileMetadata.put(fileInfo[0].split("_")[1], fileInfo[1] + "," + fileInfo[2]);
                                fileMetadata.put(fileInfo[0].split("_")[1], fileDetail.substring(fileDetail.indexOf(";")+1));
                                serverName.fileDetail.put(fileName, fileMetadata);
                                serverName.lastUpdatedTimeStamp = Instant.now();
                            }

                        }
                    }
                }
                else if(msg.contains("CREATE: "))   {
                    System.out.println(msg);
                    if(activeServer == null || activeServer.size() == 0)    {
                        System.out.println("No Server is Up");
                        out.writeUTF("No Server is Up");
                    }
                    else    {
                        Set<Integer> tempServerList = new HashSet<>(activeServer);
                        StringBuilder msgToSend = new StringBuilder();
                        int a = 3;
                        String fileName = msg.split(";")[1].split(":")[1].trim();
                        if(fileLocationDetail.containsKey(fileName))    {
                            out.writeUTF("File already exists");
                        }
                        else {
                            while(a > 0) {
                                int activeServerId = 1;
                                int id = rand.nextInt(tempServerList.size());
                                Iterator<Integer> it = tempServerList.iterator();
                                while (it.hasNext()) {
                                    System.out.println(id + "  " + tempServerList.size() + "  " + it.next());
                                }
                                it = tempServerList.iterator();
                                while (it.hasNext() && id >= 0) {
                                    activeServerId = it.next();
                                    id--;
                                }
                                Socket serverForFileCreation = new Socket(ClientList.get(activeServerId).address, port);
                                DataInputStream in1 = new DataInputStream(serverForFileCreation.getInputStream());
                                DataOutputStream out1 = new DataOutputStream(serverForFileCreation.getOutputStream());
                                out1.writeUTF(msg);
                                String msg1 = in1.readUTF();
                                msgToSend.append(msg1 + "; ");
                                if (!msg1.contains("not created")) {
                                    String filename = msg1.split(" ")[1];
                                    Map fileMetadata = new LinkedHashMap();
                                    fileMetadata.put(filename.split("_")[1], "0," + Instant.now());
                                    ClientList.get(activeServerId).fileDetail.put(filename.split("_")[0], fileMetadata);
                                }
                                in1.close();
                                out1.close();
                                serverForFileCreation.close();
                                tempServerList.remove(activeServerId);
                                a--;
                            }
                            out.writeUTF(msgToSend.toString());
                        }
                    }
                }
                else {
                    int startOffset = -1, endOffset = -1;
                    System.out.println(msg);
                    String fileName = msg.split(";")[1].split(":")[1].trim();
                    if (fileLocationDetail.containsKey(fileName)) {
                        StringBuilder sb = new StringBuilder();

                        if (msg.contains("WRITE: ")) {
                            String fileContent = msg.split(";")[2].split(":")[1].trim();
                            for (String s : fileLocationDetail.get(fileName)) {
                                if(s.contains("00"+fileLocationDetail.get(fileName).size()+".txt"))   {
                                    int ser = Integer.valueOf(s.split(",")[1]);
                                    if(ClientList.get(ser).alive) {
                                        ClientInfo serverName = ClientList.get(ser);
                                        int fileSize = Integer.valueOf(serverName.fileDetail.get(fileName)
                                                            .get("00" + fileLocationDetail.get(fileName).size() + ".txt")
                                                            .toString().split(",")[1]);
//                                        System.out.println("File Size: " + fileSize);
                                        if(fileSize + fileContent.length() < chunkSize) {
                                            sb.append(s);
                                            sb.append(";");
                                        }
                                        else    {
                                            int id = rand.nextInt(activeServer.size());
                                            int activeServerId = 1;
                                            Iterator<Integer> it = activeServer.iterator();
                                            while(it.hasNext() && id >= 0) {
                                                activeServerId = it.next();
                                                id--;
                                            }
//                                          System.out.println("Server to create on " + activeServerId + "  " + ClientList.get(activeServerId).address);
                                            Socket serverForFileCreation = new Socket(ClientList.get(activeServerId).address, port);
                                            DataInputStream in1 = new DataInputStream(serverForFileCreation.getInputStream());
                                            DataOutputStream out1 = new DataOutputStream(serverForFileCreation.getOutputStream());
                                            int chunk = fileLocationDetail.get(fileName).size() + 1;
                                            out1.writeUTF("Metadata Client ID: 1; CREATE: " + fileName + "; Chunk: " + chunk + ";");
                                            String msg1 = in1.readUTF();
//                                          System.out.println("Received: " + msg1);
                                            if (!msg1.contains("not created")) {
                                                out.writeUTF("00" + chunk + ".txt," + activeServerId + ";");
                                            }
                                            else    {
                                                out.writeUTF("Unable to Write currently, Try again later");
                                            }
                                            in1.close();
                                            out1.close();
                                            serverForFileCreation.close();
                                            System.out.println("Create new chunk");
                                        }
                                    }
                                    else    {
                                        sb.append("Chunk Not Available");
                                    }
                                }
                            }
                            out.writeUTF(sb.toString());
                        } else if (msg.contains("READ: ")) {
                            startOffset = Integer.valueOf(msg.split(";")[2].split(":")[1].trim());
                            endOffset = Integer.valueOf(msg.split(";")[3].split(":")[1].trim());
                            int startChunk = (startOffset / chunkSize) + 1;
                            int endChunk = (endOffset / chunkSize) + 1;
                            for (String s : fileLocationDetail.get(fileName)) {
                                if(s.contains(startChunk+".txt") && startChunk <= endChunk) {
                                    int ser = Integer.valueOf(s.split(",")[1]);
                                    if(ClientList.get(ser).alive) {
                                        sb.append(s);
                                        sb.append(";");
                                        startChunk++;
                                    }
                                }
                            }
                            out.writeUTF(sb.toString());
                        } else {
                            out.writeUTF("Operation not supported");
                        }
                    }
                    else    {
                        out.writeUTF("invalid filename provided");
                    }
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                System.out.println("error: " + ex.getMessage());
            }
            pool.shutdown();
        }
    }

    public void run()   {

        Set<Integer> serverList = ClientList.keySet();
        Instant currentTime = Instant.now();
        for(int c : serverList)  {
            if(ClientList.get(c).lastUpdatedTimeStamp != null) {
                long duration = Duration.between(ClientList.get(c).lastUpdatedTimeStamp, currentTime).getSeconds();
                if (duration > 15) {
                    ClientList.get(c).alive = false;
                    System.out.println("Server #" + ClientList.get(c).id + " is down");
                    activeServer.remove(c);
                }
            }
            else    {
                System.out.println("Server #" + ClientList.get(c).id + " is down");
            }
        }
    }
}
