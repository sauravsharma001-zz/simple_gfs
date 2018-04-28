/**
 *
 * WriteFile.java - Insert a new line at the end of a given file by TCPServer
 * @author  Saurav Sharma
 *
 */

package com.utd.aos.util;

import java.io.*;
import java.util.concurrent.Callable;

public class WriteFile implements Callable {

    private String directoryPath;
    private String fileName;
    private String newLine;

    public WriteFile(String directoryPath, String fileName, String newLine)  {
        this.directoryPath = directoryPath;
        this.fileName = fileName;
        this.newLine = newLine;
    }

    public Boolean call()   {
        try {
            // Assume default encoding.
            FileReader fileReader = new FileReader(directoryPath + "/" + fileName);
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            String line = "", content = "";
            while ((line = bufferedReader.readLine())!= null) {
                content += line + "\r\n";
            }

            bufferedReader.close();
            fileReader.close();

            content += newLine;

            FileWriter fileWriter = new FileWriter(directoryPath + "/" + fileName);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

            // append a newline character.
            bufferedWriter.write(content);
            bufferedWriter.newLine();
            bufferedWriter.close();
            fileWriter.close();
            return true;
        }
        catch(IOException ex) {
            System.out.println("Error writing to file '" + fileName + "'");
            return false;
        }
    }
}
