package com.phaeris.flink.task;

import cn.hutool.core.text.CharSequenceUtil;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.phaeris.flink.config.DatasourceConfig;
import com.phaeris.flink.config.KafkaConfig;
import com.phaeris.flink.constants.EnvConstants;
import com.phaeris.flink.constants.KafkaStreamConstants;
import com.phaeris.flink.task.handler.HandlerFactory;
import com.phaeris.flink.task.handler.IBaseHandler;
import com.phaeris.flink.task.mapping.AbstractTableField;
import com.phaeris.flink.task.mapping.JsonFieldMapping;
import com.phaeris.flink.sink.CommonDBSink;
import com.phaeris.flink.util.KafkaUtils;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
        String env = parameterTool.get(EnvConstants.ENV);
        if (CharSequenceUtil.isBlank(env)) {
            throw new IllegalArgumentException("运行环境未指定");
        }
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

    /**
     * 通用任务初始化执行器
     *
     * @author wyh
     * @since 2024/4/10
     */
    @Slf4j
    public static class TaskInitializer {

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

    /**
     * @author wyh
     * @since 2024/4/17
     */
    public static class ListAgg<IN> implements AggregateFunction<IN, List<IN>, List<IN>> {

        @Override
        public List<IN> createAccumulator() {
            return new ArrayList<>();
        }

        @Override
        public List<IN> add(IN entity, List<IN> list) {
            list.add(entity);
            return list;
        }

        @Override
        public List<IN> getResult(List<IN> list) {
            return list;
        }

        @Override
        public List<IN> merge(List<IN> entityList, List<IN> acc1) {
            acc1.addAll(entityList);
            return acc1;
        }
    }

    /**
     * @author wyh
     * @since 2024/4/17
     */
    @Slf4j
    public static class ProcessingTimeTrigger extends Trigger<Object, TimeWindow> {

        /**
         * 窗口最大的触发数量
         */
        private final int maxCount;

        /**
         * 记录窗口数据量
         */
        public static AtomicInteger count = new AtomicInteger(0);


        public ProcessingTimeTrigger(int maxCount) {
            this.maxCount = maxCount;
        }

        @Override
        public TriggerResult onElement(Object o, long l, TimeWindow timeWindow, TriggerContext triggerContext) {
//        log.info("onElement with count: {} " ,  count.getAndIncrement());
            count.getAndIncrement();
            if (count.get() >= maxCount) {
                log.info("count.getAndSet: {}", count.getAndSet(0));
                return TriggerResult.FIRE_AND_PURGE;
            }
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) {
            return TriggerResult.FIRE;
        }

        @Override
        public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) {
            return TriggerResult.FIRE;
        }

        @Override
        public void clear(TimeWindow timeWindow, TriggerContext triggerContext) {
            log.info("count.getAndSet:{}", count.getAndSet(0));
        }
    }
}
