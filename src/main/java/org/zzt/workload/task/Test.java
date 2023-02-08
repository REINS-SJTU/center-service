package org.zzt.workload.task;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;


public class Test {
    public static void main(String[] args) throws Exception{
        System.out.println(System.currentTimeMillis());
        String s = "a'%%s%%'b";
        System.out.println(s.replaceAll("'[\\S]*'", "''"));
        Template template = new Template();
        String t = "select L_RETURNFLAG, L_LINESTATUS, sum(L_QUANTITY) as qty, sum(L_EXTENDEDPRICE) as sum_base_price, count(*) as count_order from lineitem l group by L_RETURNFLAG, L_LINESTATUS";
        System.out.println(template.sqlToMV(t));
        BufferedReader bufferedReader = new BufferedReader(new FileReader("static"));
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("predict"));
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            String mv = template.sqlToMV(line);
            if (mv.equals("")) {
                System.out.println(line);
            } else {
                System.out.println(mv);
                bufferedWriter.write(mv);
                bufferedWriter.newLine();
            }
        }
        bufferedReader.close();
        bufferedWriter.close();
    }
}
