package com.phaeris.flink.task;

import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.phaeris.flink.agg.ListAgg;
import com.phaeris.flink.config.KafkaConfig;
import com.phaeris.flink.handler.HandlerFactory;
import com.phaeris.flink.handler.IBaseHandler;
import com.phaeris.flink.mapping.AbstractTableField;
import com.phaeris.flink.sink.CommonDBSink;
import com.phaeris.flink.trigger.ProcessingTimeTrigger;
import com.phaeris.flink.util.KafkaUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * 通用任务初始化执行器
 *
 * @author wyh
 * @since 2024/4/10
 */
@Slf4j
public class TaskInitializer {

    private final String runnerName;

    private final List<TaskContext> taskContexts;

    public TaskInitializer(String runnerName, List<TaskContext> taskContexts) {
        this.runnerName = runnerName;
        this.taskContexts = taskContexts;
    }

    public void start() throws Exception {

        //获取流程执行环境
        StreamExecutionEnvironment evnStream = StreamExecutionEnvironment.getExecutionEnvironment();

        taskContexts.forEach(context -> {

            if (Objects.isNull(context.getDbConfig()) || Objects.isNull(context.getHandlerClazz())) {
                log.error("初始化信息缺失，请检查参数");
                throw new IllegalArgumentException("初始化信息缺失，请检查参数");
            }

            String handlerName = context.getHandlerClazz().getName();
            log.info("[任务{}] 开始初始化", handlerName);

            //设置重启策略
            evnStream.setRestartStrategy(defaultRestartStrategy(context));
            //设置检查点
            if (context.isEnableCheckPoint()) {
                evnStream.enableCheckpointing(context.getCheckPointInterval());
            }

            //设置操作器
            List<SingleOutputStreamOperator<AbstractTableField>> streamOperators = Lists.newArrayList();
            Map<String, ? extends IBaseHandler> handlerMap = HandlerFactory.getHandlerMap(context.getHandlerClazz());
            for (Map.Entry<String, ? extends IBaseHandler> handlerEntry : handlerMap.entrySet()) {
                try {
                    SingleOutputStreamOperator<AbstractTableField> operator = evnStream
                            .addSource(KafkaUtils.getStringFlinkKafkaConsumer(context.getStartupModel(),
                                    //topic
                                    StrUtil.join(".", context.getTopicPrefix(), handlerEntry.getKey()),
                                    //host
                                    KafkaConfig.getBrokers(context.getEnv()),
                                    //groupId
                                    StrUtil.join(".", handlerName, context.getTaskVersion()))
                            ).map(x -> context.getFieldMapping().biMap(handlerEntry.getKey(), x));
                    streamOperators.add(operator);
                } catch (Exception e) {
                    log.info("[任务{}] 出现异常 ==> \n", handlerName, e);
                }
            }
            if (streamOperators.isEmpty()) {
                log.info("[任务{}] 没有需要执行的操作", handlerName);
                return;
            }

            //组装stream
            DataStream<AbstractTableField> combinedStream = null;
            for (SingleOutputStreamOperator<AbstractTableField> operator : streamOperators) {
                if (combinedStream == null) {
                    combinedStream = operator;
                } else {
                    combinedStream = combinedStream.union(operator);
                }
            }
            combinedStream.filter(Objects::nonNull)
                    .keyBy(AbstractTableField::getTable)
                    .window(defaultTimeWindow(context))
                    .trigger(new ProcessingTimeTrigger(context.getMaxCount()))
                    .aggregate(new ListAgg<>())
                    .addSink(new CommonDBSink(context.getDbConfig(), handlerMap))
                    .setParallelism(context.getParallelism());
        });

        //开始执行
        evnStream.execute(runnerName);
    }


    private RestartStrategies.RestartStrategyConfiguration defaultRestartStrategy(TaskContext context) {
        return RestartStrategies.failureRateRestart(
                context.getFailureRate(),
                Time.of(context.getFailureInterval(), TimeUnit.MINUTES),
                Time.of(context.getDelayInterval(), TimeUnit.SECONDS)
        );
    }

    private TumblingProcessingTimeWindows defaultTimeWindow(TaskContext context) {
        return TumblingProcessingTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(context.getTimeWindows()));
    }
}
