package com.phaeris.flink.task.impl;

import cn.hutool.core.util.StrUtil;
import com.phaeris.flink.constants.EnvConstants;
import com.phaeris.flink.task.handler.impl.UserHandle;
import com.phaeris.flink.task.mapping.impl.UserFieldMapping;
import com.phaeris.flink.task.TaskContext;
import com.phaeris.flink.task.TaskRunner;

/**
 * @author wyh
 * @since 2024/4/17
 */
public class UserIncomeTask {

    public static void main(String[] args) throws Exception {
        TaskContext context = TaskRunner.buildContext(args, UserHandle.class, new UserFieldMapping(),
                dbConfig -> StrUtil.join(".", EnvConstants.ODS_V1, dbConfig.getEnv(), dbConfig.getOutDbConfig()));
        TaskRunner.start(UserIncomeTask.class.getName(), context);
    }
}
