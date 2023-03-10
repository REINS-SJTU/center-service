package org.zzt.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zzt.entity.Metadata;
import org.zzt.mapper.MetadataMapper;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
public class MetadataServiceImpl implements MetadataService{
    MetadataMapper metadataMapper;
    @Autowired
    public MetadataServiceImpl(MetadataMapper metadataMapper) {
        this.metadataMapper = metadataMapper;
    }

    @Override
    public Boolean createMaterializedView(String viewName, String createSql, String types) {
        log.info("create mv:{}, {}", viewName, createSql);
        // check whether this name exist
        Metadata metadata = new Metadata();
        metadata.setName(viewName);
        metadata.setCreateSql(createSql);
        metadata.setCreateTime(System.currentTimeMillis());
        metadata.setAccessTime(System.currentTimeMillis());
        metadata.setVersion(1);
        metadata.setColumnTypes("");
        // default
        metadata.setTtl(300 * 60 * 1000); // 300min

        LambdaQueryWrapper<Metadata> queryWrapper = new LambdaQueryWrapper<Metadata>()
                .eq(Metadata::getName, viewName);
        Metadata ret = metadataMapper.selectOne(queryWrapper);
        if (ret != null) {
            log.info("update existing mv");
            metadata.setVersion(ret.getVersion() + 1);
            return metadataMapper.update(metadata, queryWrapper) == 1;
        } else {
            log.info("create new mv");
            return metadataMapper.insert(metadata) == 1;
        }
    }

    @Override
    public JSONObject loadMV(String viewName) {
        LambdaQueryWrapper<Metadata> queryWrapper = new LambdaQueryWrapper<Metadata>()
                .eq(Metadata::getName, viewName);
        Metadata ret = metadataMapper.selectOne(queryWrapper);
        log.info("query result {}", ret);
        if (ret == null){
            return new JSONObject();
        }else{
            Metadata newMeta = new Metadata(ret);
            // just let it expire
            newMeta.setAccessTime(System.currentTimeMillis());
            metadataMapper.update(newMeta, queryWrapper);
            log.info("update to {}", newMeta);
            return (JSONObject) JSONObject.parse(JSON.toJSONString(ret));
        }
    }

    @Override
    public Boolean isExpire(String viewName) {
        LambdaQueryWrapper<Metadata> queryWrapper = new LambdaQueryWrapper<Metadata>()
                .eq(Metadata::getName, viewName);
        Metadata ret = metadataMapper.selectOne(queryWrapper);
        if (ret == null) {
            log.info("{} not exists. equal to expire", viewName);
            return true;
        }
        return System.currentTimeMillis() - ret.getCreateTime() > ret.getTtl();
    }

    @Override
    public Long ExpireTime(String viewName) {
        LambdaQueryWrapper<Metadata> queryWrapper = new LambdaQueryWrapper<Metadata>()
                .eq(Metadata::getName, viewName);
        Metadata ret = metadataMapper.selectOne(queryWrapper);
        if (ret != null) {
            long time = System.currentTimeMillis() - ret.getCreateTime() - ret.getTtl();
            return time >= 0L ? time:0L;
        }
        return 0L;
    }

    @Override
    public JSONObject loadAllName() {
        List<String> names = loadAllMeta().stream().map(Metadata::getName).collect(Collectors.toList());
        return new JSONObject(){{
            put("data", JSONArray.parseArray(JSON.toJSONString(names)));
        }};
    }

    @Override
    public List<Metadata> loadAllMeta() {
        return metadataMapper.selectList(null);
    }
}
