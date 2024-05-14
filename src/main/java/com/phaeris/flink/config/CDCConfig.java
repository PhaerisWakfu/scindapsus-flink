package com.phaeris.flink.config;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

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


    public static CDCConfig fill(ResourceBundle bundle, String prefix) {
        CDCConfigBuilder builder = CDCConfig.builder()
                .sourceType(Integer.valueOf(bundle.getString(String.format(JDBC_DIALECT_FORMAT, prefix))))
                .host(bundle.getString(String.format(JDBC_HOST_FORMAT, prefix)))
                .port(Integer.valueOf(bundle.getString(String.format(JDBC_PORT_FORMAT, prefix))))
                .database(bundle.getString(String.format(JDBC_DATABASE_FORMAT, prefix)))
                .username(bundle.getString(String.format(JDBC_USERNAME_FORMAT, prefix)))
                .password(bundle.getString(String.format(JDBC_PASSWORD_FORMAT, prefix)));
        //option
        String schema = null;
        try {
            schema = bundle.getString(String.format(JDBC_SCHEMA_FORMAT, prefix));
        } catch (MissingResourceException exception) {
            //do nothing
        }
        builder.schema(schema);
        return builder.build();
    }
}
