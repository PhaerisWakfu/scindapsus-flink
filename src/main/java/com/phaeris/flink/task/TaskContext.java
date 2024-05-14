package com.phaeris.flink.task;

import com.phaeris.flink.config.DatasourceConfig;
import com.phaeris.flink.handler.IBaseHandler;
import com.phaeris.flink.mapping.JsonFieldMapping;
import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

/**
 * @author wyh
 * @since 2024/4/10
 */
@Data
@Builder
public class TaskContext implements Serializable {

    private static final long serialVersionUID = 8789282692714376733L;

    /**
     * 环境
     */
    private String env;

    /**
     * 业务处理器类型
     */
    private Class<? extends IBaseHandler> handlerClazz;

    /**
     * 表字段映射
     */
    private JsonFieldMapping fieldMapping;

    /**
     * 数据库配置
     */
    private DatasourceConfig dbConfig;

    /**
     * kafka topic前缀
     */
    private String topicPrefix;

    /**
     * 任务版本号
     */
    private String taskVersion;

    /**
     * 消息监听模式
     */
    private String startupModel;

    @Builder.Default
    private boolean enableCheckPoint = true;

    @Builder.Default
    private long checkPointInterval = 120000;

    @Builder.Default
    private int maxCount = 100;

    @Builder.Default
    private int failureRate = 3;

    @Builder.Default
    private long failureInterval = 5;

    @Builder.Default
    private long delayInterval = 10;

    @Builder.Default
    private long timeWindows = 10;

    @Builder.Default
    private int parallelism = 1;
}
