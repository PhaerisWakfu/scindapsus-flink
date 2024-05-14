package com.phaeris.flink.config;

import com.phaeris.flink.constants.PropertiesConstants;
import com.phaeris.flink.util.PropertiesUtil;

/**
 * @author wyh
 * @since 2024/3/15
 */
public class KafkaConfig {

    public static String getBrokers(String env) {
        return PropertiesUtil.get(env).getString(PropertiesConstants.KAFKA_BROKERS);
    }
}
