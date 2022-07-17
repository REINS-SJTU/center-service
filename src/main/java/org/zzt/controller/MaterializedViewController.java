package org.zzt.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.zzt.entity.Metadata;
import org.zzt.mapper.MetadataMapper;
import org.zzt.service.MetadataService;

import java.util.List;
import java.util.Map;

@Slf4j
@RestController
public class MaterializedViewController {
    MetadataService metadataService;
    @Autowired
    public MaterializedViewController(MetadataService metadataService) {
        this.metadataService = metadataService;
    }

    @GetMapping("/getAll")
    public String getAllMaterializedViews() {
        List<Metadata> data = metadataService.getAll();
        return data.toString();
    }

    @PostMapping("/create")
    public String create(@RequestBody Map<String, Object> data) {
        log.info(data.toString());
        JSONObject s = JSONObject.parseObject("123");
        String viewName = (String) data.get("viewName");
        String createSql = (String) data.get("createSql");
        String columnTypes = (String) data.get("columnTypes");
        if (viewName == null || createSql == null || columnTypes == null) {
            return "fail.request body error.";
        }
        Boolean status = metadataService.createMaterializedView(viewName, createSql, columnTypes);
        return status? "success":"fail";
    }

    @GetMapping("/loadMV")
    public JSONObject loadMV(@RequestParam String viewName) {
        log.info(viewName);
        JSONObject ret = metadataService.loadMV(viewName);
        log.info(ret.toString());
        return ret;
    }
}
