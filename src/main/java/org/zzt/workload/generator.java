package org.zzt.workload;

import org.zzt.workload.task.Task;

import java.io.BufferedWriter;
import java.io.FileWriter;

public class generator {
    public static void main(String[] args) throws Exception {
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("static"));
        int round = 25, index = 12;
        for(int i = 0; i < 25; i++) {

            for (int j = 0; j < 12; ++j){
                String sql = Task.tasks[j].sql();
                bufferedWriter.write(sql);
                bufferedWriter.newLine();
            }
        }
        bufferedWriter.close();
    }
}
