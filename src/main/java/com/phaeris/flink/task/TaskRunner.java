package com.phaeris.flink.task;

import com.google.common.collect.Lists;
import com.phaeris.flink.config.DatasourceConfig;
import com.phaeris.flink.constants.EnvConstants;
import com.phaeris.flink.constants.KafkaStreamConstants;
import com.phaeris.flink.handler.IBaseHandler;
import com.phaeris.flink.mapping.JsonFieldMapping;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.function.Function;

/**
 * @author wyh
 * @since 2024/4/15
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TaskRunner {

    /**
     * 任务入口
     *
     * @param runnerName   main方法参数
     * @param taskContexts 任务上下文
     * @throws Exception 执行异常
     */
    public static void start(String runnerName, TaskContext... taskContexts) throws Exception {
        new TaskInitializer(runnerName, Lists.newArrayList(taskContexts)).start();
    }

    /**
     * 构建任务上下文
     *
     * @param args         main方法参数
     * @param handlerClazz 执行的handler类型
     * @param fieldMapping 字段映射
     * @param topicFunc    topic函数
     */
    public static TaskContext buildContext(String[] args, Class<? extends IBaseHandler> handlerClazz,
                                           JsonFieldMapping fieldMapping, Function<DatasourceConfig, String> topicFunc) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String env = parameterTool.get(EnvConstants.ENV, EnvConstants.TEST);
        String startUpModel = parameterTool.get(EnvConstants.STARTUP_MODEL, KafkaStreamConstants.LATEST);
        String taskVersion = parameterTool.get(EnvConstants.TASK_VERSION, EnvConstants.DEFAULT_TASK_VERSION);
        log.info("[执行任务{}] 执行环境: [{}]", handlerClazz.getName(), env);
        DatasourceConfig dbConfig = new DatasourceConfig(env);
        return TaskContext.builder()
                .env(env)
                .handlerClazz(handlerClazz)
                .fieldMapping(fieldMapping)
                .dbConfig(dbConfig)
                .topicPrefix(topicFunc.apply(dbConfig))
                .taskVersion(taskVersion)
                .startupModel(startUpModel)
                .build();
    }
}
