package org.zzt.service;

import com.alibaba.fastjson.JSONObject;
import org.springframework.stereotype.Service;
import org.zzt.entity.Metadata;

import java.util.List;

@Service
public interface MetadataService {
    public Boolean createMaterializedView(String viewName, String createSql, String types);
    public JSONObject loadMV(String viewName);
    public Boolean isExpire(String viewName);
    public Long ExpireTime(String viewName);
    JSONObject loadAllName();
    public List<Metadata> loadAllMeta();
}
