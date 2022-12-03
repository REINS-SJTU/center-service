package org.zzt.workload;

import java.io.BufferedReader;
import java.io.FileReader;

public class Submit {
    public static void main(String[] args) throws Exception{
        BufferedReader bufferedReader = new BufferedReader(new FileReader("static"));
        String line = null;
        while ((line = bufferedReader.readLine()) != null) {
            System.out.println(line);
            Process process =  Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", "./test.sh", line});
            int returnCode = process.waitFor();
        }
        bufferedReader.close();
    }
}
