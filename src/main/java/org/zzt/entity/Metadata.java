package org.zzt.entity;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

@Data
@TableName("metadata")
public class Metadata {
    @TableId
    private Long id;
    private String name;
    private String createSql;
    private Long createTime;
    private Long accessTime;
    private Integer version;
    private String columnTypes;
}
