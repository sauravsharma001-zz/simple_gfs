/**
 * CreateFile.java - Creates a file on a given server by TCPServer
 *
 * @author Saurav Sharma and Amal Roy
 */

package com.utd.aos.util;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;

public class CreateFile implements Callable {

    private static String directoryPath;
    private static String fileName;

    public CreateFile(String directoryPath, String fileName) {
        this.fileName = fileName;
        this.directoryPath = directoryPath;
    }

    @Override
    public String call() {
        try {

            File file = new File(directoryPath + "/" + fileName);
            if(file.exists())   {
                return "File Exists";
            }
            else if (file.createNewFile()){
               return "File " + fileName + " created";
            }else{
                return "File not created";
            }

        } catch (IOException e) {
            e.printStackTrace();
            return "File not created";
        }
    }
}
