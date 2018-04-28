/**
 *
 * Main.java - Entry point
 * @author: Saurav Sharma and Amal Roy
 *
 * @args: <configuration filepath> <server or metadata_server or client> <server or metadata_server or client ID>
 *
 */

package com.utd.aos;

import com.utd.aos.client.TCPClient;
import com.utd.aos.metadata.TCPMetadataServer;
import com.utd.aos.server.TCPServer;
import com.utd.aos.util.ReadPropertyFile;

import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

public class Main {

    public static void main(String[] args) {

        if(args.length == 3) {
            try {
                String clientOrServer = args[1];
                String filePath = args[0];
                String clientOrServerID = args[2];
                Properties prop = ReadPropertyFile.readProperties(filePath);

                if (clientOrServer.equalsIgnoreCase("client")) {
                    TCPClient client = new TCPClient(prop, clientOrServerID);
                    client.startClient();
                }
                else if (clientOrServer.equalsIgnoreCase("server")){
                    TCPServer server = new TCPServer(prop, clientOrServerID);
                    Timer timer = new Timer();
                    timer.schedule(server, 0, 5000);
                    server.startServer();
                }
                else if (clientOrServer.equalsIgnoreCase("metadata_server")){
                    TCPMetadataServer metadataServer = new TCPMetadataServer(prop);
                    Timer timer = new Timer();
                    timer.schedule(metadataServer, 5000, 5000);
                    metadataServer.startServer();
                }
                else {
                    System.out.println("Only 'server' or 'client' or 'metadata_server' is accepted as valid input");
                }
            }
            catch (Exception ex)    {
                ex.printStackTrace();
            }
        }
        else    {
            System.out.println("Invalid Syntax");
            System.out.println("Usage: <property file> <server or client> <server/client id>");
        }
    }
}
