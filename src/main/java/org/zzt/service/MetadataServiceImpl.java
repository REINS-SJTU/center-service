package org.zzt.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zzt.entity.Metadata;
import org.zzt.mapper.MetadataMapper;

import java.util.List;

@Service
public class MetadataServiceImpl implements MetadataService{
    MetadataMapper metadataMapper;
    @Autowired
    public MetadataServiceImpl(MetadataMapper metadataMapper) {
        this.metadataMapper = metadataMapper;
    }

    @Override
    public Boolean createMaterializedView(String viewName, String createSql, String types) {
        Metadata metadata = new Metadata();
        metadata.setName(viewName);
        metadata.setCreateSql(createSql);
        metadata.setCreateTime(System.currentTimeMillis());
        metadata.setAccessTime(System.currentTimeMillis());
        metadata.setVersion(1);
        metadata.setColumnTypes(types);
        int ret = metadataMapper.insert(metadata);
        return ret == 1;
    }

    @Override
    public JSONObject loadMV(String viewName) {
        LambdaQueryWrapper<Metadata> queryWrapper = new LambdaQueryWrapper<Metadata>()
                .eq(Metadata::getName, viewName);
        Metadata ret = metadataMapper.selectOne(queryWrapper);
        if (ret == null){
            return new JSONObject();
        }else{
            return (JSONObject) JSONObject.parse(JSON.toJSONString(ret));
        }

    }

    @Override
    public List<Metadata> getAll() {
        return metadataMapper.selectList(null);
    }
}
