package org.zzt.entity;

import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

@Data
@TableName("metadata")
public class Metadata {
    @TableId
    private Long id;
    public String name;
    public String createSql;
    private Long createTime;
    private Long accessTime;
    private Integer version;
    private String columnTypes;
    private Integer ttl;

    public Metadata(){}
    public Metadata(Metadata metadata) {
        this.setId(metadata.id);
        this.setName(metadata.name);
        this.setCreateSql(metadata.createSql);
        this.setCreateTime(metadata.createTime);
        this.setAccessTime(metadata.accessTime);
        this.setVersion(metadata.version);
        this.setColumnTypes(metadata.columnTypes);
        this.setTtl(metadata.ttl);
    }
}
