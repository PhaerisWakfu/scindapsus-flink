package com.phaeris.flink.enums;

import cn.hutool.core.text.CharSequenceUtil;
import com.phaeris.flink.util.EnumUtil;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author wyh
 * @since 2023/4/23
 */
@Getter
@AllArgsConstructor
public enum DatasourceEnum {

    MYSQL(1, "com.mysql.cj.jdbc.Driver") {
        @Override
        public String getUrl(String host, Integer port, String database, String schema) {
            return String.format("jdbc:mysql://%s:%s/%s?useSSL=false&useUnicode=true&characterEncoding=UTF-8&autoReconnect=true&serverTimezone=GMT%%2B8&zeroDateTimeBehavior=convertToNull",
                    host, port, database);
        }
    },

    POSTGRESQL(2, "org.postgresql.Driver") {
        @Override
        public String getUrl(String host, Integer port, String database, String schema) {
            String url = "jdbc:postgresql://%s:%s/%s?useSSL=true&useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai&stringtype=unspecified&reWriteBatchedInserts=true&ApplicationName=weihaiFlink";
            if (CharSequenceUtil.isNotBlank(schema)) {
                url += "&currentSchema=" + schema;
            }
            return String.format(url, host, port, database);
        }
    };

    private final Integer value;

    private final String driverClassName;

    /**
     * 根据value获取枚举，取不到默认{@link this#MYSQL}
     *
     * @param sourceType 数据源类型
     * @return this
     */
    @SuppressWarnings("unused")
    public static DatasourceEnum getByValue(Integer sourceType) {
        return EnumUtil.valueOf(DatasourceEnum.class, sourceType, DatasourceEnum::getValue, MYSQL);
    }

    /**
     * 获取jdbc url
     *
     * @param host     主机名
     * @param port     端口
     * @param database 库名
     * @param schema   pg schema 名
     * @return jdbc url
     */
    public abstract String getUrl(String host, Integer port, String database, String schema);
}
