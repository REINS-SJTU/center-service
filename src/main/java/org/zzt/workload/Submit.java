package org.zzt.workload;

import org.zzt.workload.task.Template;

import java.io.*;

public class Submit {
    public static void main(String[] args) throws Exception{
        BufferedReader bufferedReader = new BufferedReader(new FileReader("static"));
        String line = null;
        int count = 0;
        while ((line = bufferedReader.readLine()) != null) {
            System.out.println(line);
            String[] cmd = {"./test.sh", "0", "true", Template.index2MV.get((count % 12) + 1), line};
            Process process =  Runtime.getRuntime().exec(cmd);
            BufferedReader out = new BufferedReader(new InputStreamReader(process.getInputStream()));
            // System.out.println(process.waitFor());
            String outLine = null;
            while((outLine = out.readLine()) != null) {
                System.out.println(outLine);
            }
            out.close();
            count++;
//            if (count >= 12) {
//                break;
//            }
            Thread.sleep(20 * 1000);
        }
        bufferedReader.close();
    }
}
