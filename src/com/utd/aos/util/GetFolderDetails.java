/**
 *
 * ReadPropertyFile.java - Get File Details present under Server Directory for TCPServer
 * @author  Saurav Sharma and Amal Roy
 *
 */

package com.utd.aos.util;

import java.io.File;
import java.util.concurrent.Callable;

public class GetFolderDetails  implements Callable {

    private static String directoryPath;
    public GetFolderDetails(String directoryPath)  {
        this.directoryPath = directoryPath;
    }

    @Override
    public String call() throws Exception {
        try {
            File folder = new File(directoryPath);
            File[] listOfFiles = folder.listFiles();

            StringBuilder sb = new StringBuilder();

            for (int i = 0; i < listOfFiles.length; i++) {
                if (listOfFiles[i].isFile()) {

                    sb.append(listOfFiles[i].getName());
                    sb.append(",");
                    sb.append(listOfFiles[i].length());
                    sb.append(",");
                    sb.append(listOfFiles[i].lastModified());
                    sb.append(";");
                }
            }
            return sb.toString();
        }   catch (Exception ex)    {
            ex.printStackTrace();
            return "error occurred unable to read directory";
        }
    }
}
