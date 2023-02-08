package org.zzt.workload;

import org.zzt.workload.task.Task;
import org.zzt.workload.task.Template;

import java.io.BufferedWriter;
import java.io.FileWriter;

public class generator {
    public static void main(String[] args) throws Exception {
//        preCompute();
        staticWorkload();
    }
    public static void staticWorkload() throws Exception {
        Template template = new Template();
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("static_workload"));
        // 80 vs 20
        int round = 20, index = 12;
        for(int i = 0; i < round; i++) {
            for (int j = 0; j < index; ++j){
                String sql = Task.tasks[j].sql();
                bufferedWriter.write(sql);
//                Integer t = template.mvToIndex.get(template.sqlToMV(sql));
//                bufferedWriter.write(template.mvSqlToName.get(template.sqlToMV(sql)));
                bufferedWriter.newLine();
            }
        }
        bufferedWriter.close();
    }

    public static void preCompute() throws Exception {
        Template template = new Template();
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("static_precompute"));
        int round = 1;
        for(int i = 0; i < round; i++) {
            for (int j = 0; j < 12; ++j){
                String sql = Task.tasks[j].sql();
                String precomputeSql = template.sqlToMV(sql);
                if (precomputeSql.equals("")) {
                    System.out.println("precompute sql error.");
                    continue;
                }
                bufferedWriter.write(precomputeSql);
                bufferedWriter.newLine();
                bufferedWriter.write(sql);
                bufferedWriter.newLine();
            }
        }
        bufferedWriter.close();
    }
}
