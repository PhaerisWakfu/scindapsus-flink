package com.phaeris.flink.util;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;

/**
 * @author wyh
 * @since 2021/4/6 14:51
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class JdbcUtil {

    /**
     * 测试连接
     *
     * @param url             数据库连接
     * @param driverClassName 驱动名称
     * @param username        账号
     * @param password        密码
     * @return 数据源
     */
    @SuppressWarnings("unused")
    public static boolean testSource(String url, String driverClassName, String username, String password) {

        try {
            Class.forName(driverClassName);
        } catch (Exception e) {
            log.error(e.toString(), e);
            return false;
        }

        try (Connection con = DriverManager.getConnection(url, username, password)) {
            return con != null;
        } catch (Exception e) {
            log.error(e.toString(), e);
        }
        return false;
    }

    /**
     * 获取数据源
     *
     * @param url             数据库连接
     * @param driverClassName 驱动名称
     * @param username        账号
     * @param password        密码
     * @return 数据源
     */
    public static DataSource getDataSource(String url, String driverClassName, String username, String password) {
        //默认配置poolSize(10)一般就足够支撑6000TPS
        //poolSize不是越大越好, 详见hikari作者的文章(https://github.com/brettwooldridge/HikariCP/wiki/About-Pool-Sizing)
        //推荐连接数 = (cpu核数 * 2) + 磁盘有效主轴数
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(url);
        hikariConfig.setDriverClassName(driverClassName);
        hikariConfig.setUsername(username);
        hikariConfig.setPassword(password);
        //当minIdle=maxPoolSize时, idleTimeout无用
        //设置maxLifetime小于数仓最大空闲时间(只有空闲的连接才会被移除)
        hikariConfig.setMaxLifetime(170000);
        return new HikariDataSource(hikariConfig);
    }
}