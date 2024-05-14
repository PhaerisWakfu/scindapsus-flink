package com.phaeris.flink.util;

import com.phaeris.flink.constants.KafkaStreamConstants;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author wyh
 * @since 2024/3/15
 */
public class KafkaUtils {

    private static final Logger log = LoggerFactory.getLogger(KafkaUtils.class);

    public static Properties getKafkaConfig(String brokers, String groupId) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", brokers);
        props.setProperty("group.id", groupId);
        props.setProperty("session.timeout.ms", "120000");
        props.setProperty("request.timeout.ms", "120000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    public static FlinkKafkaConsumer<String> getStringFlinkKafkaConsumer(String consumerType, String topic, String brokerList, String groupId) {
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), getKafkaConfig(brokerList, groupId));
        log.info("kafka消费地址：{} topic:{} groupId:{}", brokerList, topic, groupId);
        if (KafkaStreamConstants.LATEST.equals(consumerType)) {
            consumer.setStartFromLatest();
            log.info("kafka消费模式以最新的消费：{}", KafkaStreamConstants.LATEST);
        } else if (KafkaStreamConstants.EARLIEST.equals(consumerType)) {
            consumer.setStartFromEarliest();
            log.info("kafka消费模式根据设置最早的时间消费：{}", KafkaStreamConstants.EARLIEST);
        } else {
            consumer.setStartFromGroupOffsets();
        }
        return consumer;
    }
}
