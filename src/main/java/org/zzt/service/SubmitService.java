package org.zzt.service;

import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zzt.workload.task.Template;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.*;

@Service
public class SubmitService {
//    @Autowired
//    KafkaService kafka;
    @Autowired
    MetadataService metadataService;
    private final Template template = new Template();
    private final List<String> window = new ArrayList<>();
    private final Integer windowSize = 5;
    private Long taskGap = 20 * 1000L;
    private final Random random = new Random();
    private Integer counter = 0;
    private final Integer maxCounter = 40;
//    private Map<String, Integer> preCompute = new HashMap<>();
    public void run() throws Exception {
        BufferedReader bufferedReader = new BufferedReader(new FileReader("static_workload"));
        String line = null;
        // q1, q2, q3... qn
        while ((line = bufferedReader.readLine()) != null) {
            System.out.println(line);
            String targetMv = template.sqlToMV(line);
            String mvName = template.mvSqlToName.get(targetMv);
            String[] cmd = new String[]{"./test.sh", "0", "false", mvName, line};
            submit(cmd);
            Thread.sleep(taskGap);
            if (window.size() < windowSize) {
                window.add(mvName);
            } else {
                // full trigger predict
                window.remove(0);
                window.add(mvName);
                String next = predict();
                // run pre compute task
                System.out.println(next);
                String sql = template.mvNameToSql.get(next);
                if (sql != null) {
                    String[] preCompute = new String[]{"./test.sh", "pre_compute", "true", next, template.mvNameToSql.get(next)};
                    if (metadataService.isExpire(next)) {
                        System.out.println(next + " is expire.");
                        if (random.nextDouble() < 0.33) {
//                            submit(preCompute);
                        }
                    } else {
                        // refresh light task
                        if (!isHeavyTask(next) && random.nextDouble() < 0.1) {
//                            submit(preCompute);
                        }
//                        counter++;
//                        if (counter >= maxCounter && !isHeavyTask(next)) {
//                            // force update
//                            submit(preCompute);
//                            counter = 0;
//                        }
                    }
                } else {
                    System.out.println(next + " has empty sql.");
                }
            }
//             send to kafka
//            kafka.send("test", mvName);
            // predict

        }
    }

    private Boolean isHeavyTask(String viewName) {
        List<String> name = Arrays.asList("mv3", "mv5", "mv9", "mv10", "mv17");
        return name.contains(viewName);
    }

    private String predict() throws Exception {
        if (window.size() != windowSize) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        window.forEach(e -> {
            sb.append(",").append(e);
        });
        String arg = sb.toString();
        arg = arg.substring(1);
        String condaPython = "/Users/zzt/miniconda3/envs/test/bin/python";
        String scriptPath = "/Users/zzt/code/test/pre_compute/lstm.py";
        String[] cmd = new String[]{condaPython, scriptPath, arg}; //
        System.out.println(Arrays.toString(cmd));
        Process process =  Runtime.getRuntime().exec(cmd);
        System.out.println("execute code:" + process.waitFor());
        BufferedReader out = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line, ret = null;
        while((line = out.readLine()) != null) {
            ret = line;
//            System.out.println(line);
        }
        out.close();
        if (ret != null) {
            JSONObject jsonObject = JSONObject.parseObject(ret);
            return jsonObject.getString("ret");
        } else {
            System.out.println("predict ret is null");
            return null;
        }
    }

    private void submit(String[] cmd) throws Exception {
        Process process =  Runtime.getRuntime().exec(cmd);
        System.out.println("execute spark job code:" + process.waitFor());
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
}
