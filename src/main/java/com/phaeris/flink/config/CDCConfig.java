package com.phaeris.flink.config;

import lombok.Builder;
import lombok.Data;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.Serializable;

import static com.phaeris.flink.constants.PropertiesConstants.*;

/**
 * @author wyh
 * @since 2024/4/17
 */
@Data
@Builder
public class CDCConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * {@link com.phaeris.flink.enums.DatasourceEnum}
     */
    private Integer sourceType;

    private String host;

    private Integer port;

    private String database;

    private String schema;

    private String username;

    private String password;


    public static CDCConfig fill(ParameterTool parameterTool, String prefix) {
        CDCConfigBuilder builder = CDCConfig.builder()
                .sourceType(parameterTool.getInt(String.format(JDBC_DIALECT_FORMAT, prefix)))
                .host(parameterTool.get(String.format(JDBC_HOST_FORMAT, prefix)))
                .port(parameterTool.getInt(String.format(JDBC_PORT_FORMAT, prefix)))
                .database(parameterTool.get(String.format(JDBC_DATABASE_FORMAT, prefix)))
                .username(parameterTool.get(String.format(JDBC_USERNAME_FORMAT, prefix)))
                .password(parameterTool.get(String.format(JDBC_PASSWORD_FORMAT, prefix)))
                .schema(parameterTool.get(String.format(JDBC_SCHEMA_FORMAT, prefix)));
        return builder.build();
    }
}
