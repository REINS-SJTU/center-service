package org.zzt.controller;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.zzt.entity.Metadata;
import org.zzt.service.MetadataService;
import org.zzt.service.SparkService;

import java.util.List;
import java.util.Map;

@Slf4j
@RestController
public class MaterializedViewController {
    MetadataService metadataService;
    SparkService sparkService;
    @Autowired
    public MaterializedViewController(MetadataService metadataService, SparkService sparkService) {
        this.metadataService = metadataService;
        this.sparkService = sparkService;
    }

    @GetMapping("/getAll")
    public String getAllMaterializedViews() {
        List<Metadata> data = metadataService.loadAllMeta();
        return data.toString();
    }

    @PostMapping("/create")
    public String create(@RequestBody Map<String, Object> data) {
        log.info(data.toString());
//        JSONObject s = JSONObject.parseObject(data.toString());
        String viewName = (String) data.get("viewName");
        String createSql = (String) data.get("createSql");
        String columnTypes = (String) data.get("columnTypes");
        if (viewName == null || createSql == null || columnTypes == null) {
            return "fail. request body error.";
        }
        Boolean status = metadataService.createMaterializedView(viewName, createSql, columnTypes);
        return status? "success":"fail";
    }

    @GetMapping("/loadMV")
    public JSONObject loadMV(@RequestParam String viewName) {
        log.info("load mv:{}", viewName);
        JSONObject ret = metadataService.loadMV(viewName);
        log.info(ret.toString());
        return ret;
    }

    @GetMapping("/loadAllMV")
    public JSONObject loadAll() {
        return metadataService.loadAllName();
    }

    @GetMapping("loadAllMetadata")
    public List<Metadata> loadAllMetadata() {
        return metadataService.loadAllMeta();
    }

    @GetMapping("/spark")
    public void spark() {
        String sql =
        " select S_ACCTBAL, S_NAME, N_NAME, P_PARTKEY, P_MFGR, S_ADDRESS, S_PHONE, S_COMMENT, R_NAME, PS_SUPPLYCOST, P_SIZE, P_TYPE " +
                "from PART, SUPPLIER, PARTSUPP, NATION, REGION " +
                "where P_PARTKEY = PS_PARTKEY " +
                "and S_SUPPKEY = PS_SUPPKEY " +
                "and S_NATIONKEY = N_NATIONKEY " +
                "and N_REGIONKEY = R_REGIONKEY";
        LogicalPlan lp = sparkService.optimize(sql);
        System.out.println(1);
    }
}
