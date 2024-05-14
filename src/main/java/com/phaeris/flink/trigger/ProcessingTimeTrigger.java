package com.phaeris.flink.trigger;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author wyh
 * @since 2024/4/17
 */
@Slf4j
public class ProcessingTimeTrigger extends Trigger<Object, TimeWindow> {

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
