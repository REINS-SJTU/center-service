package org.zzt.controller;

import com.alibaba.fastjson.JSONArray;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.zzt.index.Index;
import org.zzt.service.SparkService;

import java.util.Map;
import java.util.Set;

@RestController
public class SparkController {
    @Autowired
    SparkService sparkService;
    @Autowired
    Index index;

    @PostMapping("/spark/loadCandidate")
    public JSONArray loadAll(@RequestBody Map<String, String> data) {
        if (!index.initialized) {
            index.initIndex(sparkService.loadAll());
        }

        Set<String> candidates = index.search(data.get("sql"));
        System.out.println(candidates);

        JSONArray jsonArray = new JSONArray();
        jsonArray.addAll(candidates);
        return jsonArray;
    }
}
