package org.zzt.workload;

import org.zzt.workload.task.Template;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Submit {
    public static void main(String[] args) throws Exception{
        BufferedReader bufferedReader = new BufferedReader(new FileReader("static_precompute"));
        String line = null;
        int preComputeCounter = 0;
        int sqlCounter = 0;
        List<String[]> preTasks = new ArrayList<>(), sqlTasks = new ArrayList<>(), finalTasks = new ArrayList<>();

        Template template = new Template();
        // mv1, mv2, mv3... mvn
        while ((line = bufferedReader.readLine()) != null) {
//            System.out.println(line);
//            String[] cmd = {"./test.sh", "0", "true", Template.index2MV.get((count % 12) + 1), line};
            String[] cmd;
            if (sqlCounter % 2 == 0) {
                // pre_compute
                String mvName = template.mvSqlToName.get(line);
                cmd = new String[]{"./test.sh", "pre_compute", "true", mvName, line};
                preTasks.add(cmd);
            } else {
                // real sql query
                cmd = new String[]{"./test.sh", "0", "false", Template.index2MV.get((preComputeCounter % 12) + 1), line};
                preComputeCounter++;
                sqlTasks.add(cmd);
            }
            finalTasks.add(cmd);
            sqlCounter++;
        }

        Long taskGap = 20 * 1000L; // 20s
        submit(preTasks.get(11));
//        for (String[] c: sqlTasks) {
//            submit(c);
//            System.out.println("submit:" + Arrays.toString(c));
//            Thread.sleep(taskGap);
//        }
//        while (p1 < len) {
//            submit(preTasks.get(p1++));
//            submit(sqlTasks.get(p2++));
//            Thread.sleep(taskGap);
//        }
//        while(p2 < len) {
//            submit(sqlTasks.get(p2++));
//            Thread.sleep(taskGap);
//        }
        // for init all pre_compute
//        for (String[] c: preTasks) {
//            submit(c);
//            Thread.sleep(taskGap);
//        }
        bufferedReader.close();
    }
    private static void submit(String[] cmd) throws Exception {
        Process process =  Runtime.getRuntime().exec(cmd);
        BufferedReader out = new BufferedReader(new InputStreamReader(process.getInputStream()));
        System.out.println("===task log start===");
        String outLine = null;
        while((outLine = out.readLine()) != null) {
            System.out.println(outLine);
        }
        out.close();
        System.out.println("===task log end===");
        // collect data for predicting the next 10 sql template
        // then choose the most time-consuming sql from memory(listen from kafka)
        // remove the 'expire' in spark engine
    }

    private static void send() {

    }
}
