package com.phaeris.flink.task.handler.impl;

import com.google.auto.service.AutoService;
import com.google.common.collect.Lists;
import com.phaeris.flink.entity.po.User;
import com.phaeris.flink.enums.TableEnum;
import com.phaeris.flink.task.handler.HandlerContext;
import com.phaeris.flink.mapper.UserODSMapper;
import com.phaeris.flink.mapper.UserIncomeADSMapper;
import com.phaeris.flink.task.mapping.impl.UserTableField;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author wyh
 * @since 2024/3/18
 */
@AutoService(UserHandle.class)
@SuppressWarnings("unused")
public class UserIncomeHandler implements UserHandle {

    @Override
    public void doProcess(HandlerContext context) {
        UserODSMapper outMapper = context.getOutSqlSession().getMapper(UserODSMapper.class);
        UserIncomeADSMapper sinkMapper = context.getSinkSqlSession().getMapper(UserIncomeADSMapper.class);
        List<Long> ids = context.getTableInfos().stream().map(x -> x.<UserTableField>convert().getId()).collect(Collectors.toList());
        List<User> reportPathPartJan = outMapper.getUser(ids);
        sinkMapper.insertBatch(reportPathPartJan);
    }

    @Override
    public List<TableEnum> froms() {
        return Lists.newArrayList(TableEnum.ODS.t_user);
    }
}
