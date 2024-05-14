package com.phaeris.flink.mapping;

/**
 * @author wyh
 * @since 2024/4/17
 */
public interface JsonFieldMapping {

    /**
     * 字段映射
     *
     * @param table 表名
     * @param json  表变更的json结构
     * @return 映射对象
     */
    AbstractTableField biMap(String table, String json);
}
