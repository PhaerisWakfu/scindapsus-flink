package com.phaeris.flink.task.mapping.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.phaeris.flink.task.mapping.JsonFieldMapping;

/**
 * @author wyh
 * @since 2024/4/17
 */
public class UserFieldMapping implements JsonFieldMapping {

    @Override
    public UserTableField biMap(String table, String json) {
        JSONObject jsonObject = JSON.parseObject(json);
        return new UserTableField(table, jsonObject.getLong("id"));
    }
}
