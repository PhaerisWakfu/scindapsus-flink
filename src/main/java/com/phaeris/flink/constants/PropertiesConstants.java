package com.phaeris.flink.constants;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * @author wyh
 * @since 2024/3/21
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PropertiesConstants {

    public static final String PROPERTIES_NAME_FORMAT = "application-%s";

    /*jdbc*/

    public static final String JDBC_DIALECT_FORMAT = "%s.dialect";

    public static final String JDBC_HOST_FORMAT = "%s.host";

    public static final String JDBC_PORT_FORMAT = "%s.port";

    public static final String JDBC_USERNAME_FORMAT = "%s.username";

    public static final String JDBC_PASSWORD_FORMAT = "%s.password";

    public static final String JDBC_DATABASE_FORMAT = "%s.database";

    public static final String JDBC_SCHEMA_FORMAT = "%s.schema";

    public static final String JDBC_OUT_PREFIX = "jdbc.out";

    public static final String JDBC_SINK_PREFIX = "jdbc.sink";

    public static final String MAPPER_LOCATIONS = "jdbc.mapper.locations";

    /*kafka*/

    public static final String KAFKA_BROKERS = "kafka.brokers";
}
