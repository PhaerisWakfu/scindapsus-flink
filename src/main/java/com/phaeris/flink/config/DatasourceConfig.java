package com.phaeris.flink.config;

import cn.hutool.core.util.ClassUtil;
import cn.hutool.core.util.StrUtil;
import com.phaeris.flink.enums.DatasourceEnum;
import com.phaeris.flink.util.JdbcUtil;
import com.phaeris.flink.util.PropertiesUtil;
import lombok.Data;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;

import javax.sql.DataSource;
import java.io.Serializable;
import java.util.ResourceBundle;
import java.util.Set;

import static com.phaeris.flink.constants.PropertiesConstants.*;

/**
 * @author wyh
 * @since 2024/3/15
 */
@Data
public class DatasourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private String env;

    private Set<Class<?>> mapperClazz;

    private CDCConfig outDbConfig;

    private CDCConfig sinkDbConfig;


    public DatasourceConfig(String env) {
        if (StrUtil.isBlank(env)) {
            throw new IllegalArgumentException("unknown env");
        }
        this.env = env;
        ResourceBundle bundle = PropertiesUtil.get(env);
        this.outDbConfig = CDCConfig.fill(bundle, JDBC_OUT_PREFIX);
        this.sinkDbConfig = CDCConfig.fill(bundle, JDBC_SINK_PREFIX);
        this.mapperClazz = ClassUtil.scanPackage(bundle.getString(MAPPER_LOCATIONS));
    }

    public SqlSessionFactory getSqlSessionFactory(CDCConfig config) {
        DatasourceEnum datasourceEnum = DatasourceEnum.getByValue(config.getSourceType());
        String url = datasourceEnum.getUrl(config.getHost(), config.getPort(), config.getDatabase(), config.getSchema());
        DataSource dataSource = JdbcUtil.getDataSource(url, datasourceEnum.getDriverClassName(), config.getUsername(), config.getPassword());
        //事务
        TransactionFactory transactionFactory = new JdbcTransactionFactory();
        //创建环境
        Environment environment = new Environment(datasourceEnum.name(), transactionFactory, dataSource);
        //创建配置
        Configuration configuration = new Configuration(environment);
        //开启驼峰规则
        configuration.setMapUnderscoreToCamelCase(true);
        mapperClazz.forEach(configuration::addMapper);
        return new SqlSessionFactoryBuilder().build(configuration);
    }
}
