/**
 *
 * WriteFile.java - Read the content of a given chunk file by TCPServer
 * @author  Saurav Sharma and Amal Roy
 *
 */

package com.utd.aos.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.concurrent.Callable;

public class ReadFile implements Callable {

    private String directoryPath;
    private String fileName;
    private int startOffset;
    private int endOffset;

    public ReadFile(String directoryPath, String fileName, int startOffset, int endOffset)  {
        this.directoryPath = directoryPath;
        this.fileName = fileName;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    public String call()   {

        String lastLine = "";
        try {
            FileReader fileReader = new FileReader(directoryPath + "/" + fileName);

            BufferedReader bufferedReader = new BufferedReader(fileReader);
            if (bufferedReader == null) {
                return "can't read file: " + fileName;
            }
            String line = bufferedReader.readLine();
            StringBuilder sb = new StringBuilder();
            while (line != null) {
                sb.append(line);
                sb.append("\n");
                line = bufferedReader.readLine();
            }

            // Always close files.
            bufferedReader.close();
            if(startOffset < 1) {
                startOffset = 0;
            }
            if(startOffset > sb.length()) {
                return "";
            }
            if(endOffset < 1 || endOffset > sb.length()) {
                endOffset = sb.length();
            }
            String res = sb.substring(startOffset--, endOffset);
            return res;
        }
        catch(Exception ex) {
            ex.printStackTrace();
            return "Error occurred while reading file " + fileName;
        }
    }
}
