package com.phaeris.flink.sink;

import cn.hutool.core.collection.CollUtil;
import com.phaeris.flink.config.DatasourceConfig;
import com.phaeris.flink.handler.HandlerContext;
import com.phaeris.flink.handler.IBaseHandler;
import com.phaeris.flink.mapping.AbstractTableField;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author wyh
 * @since 2024/4/17
 */
@Slf4j
public class CommonDBSink extends RichSinkFunction<List<AbstractTableField>> {

    protected DatasourceConfig dbConfig;

    protected Map<String, ? extends IBaseHandler> handleMap;

    protected SqlSessionFactory outSqlSessionFactory;

    protected SqlSessionFactory sinkSqlSessionFactory;


    public CommonDBSink(DatasourceConfig dbConfig, Map<String, ? extends IBaseHandler> handleMap) {
        this.dbConfig = dbConfig;
        this.handleMap = handleMap;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        outSqlSessionFactory = dbConfig.getSqlSessionFactory(dbConfig.getOutDbConfig());
        sinkSqlSessionFactory = dbConfig.getSqlSessionFactory(dbConfig.getSinkDbConfig());
        super.open(parameters);
    }

    @Override
    public void invoke(List<AbstractTableField> value, Context context) {
        if (CollUtil.isEmpty(value)) {
            return;
        }
        SqlSession outSqlSession = outSqlSessionFactory.openSession(true);
        SqlSession sinkSqlSession = sinkSqlSessionFactory.openSession(true);
        try {
            Map<String, List<AbstractTableField>> tableMap = value.stream().collect(Collectors.groupingBy(AbstractTableField::getTable));
            if (!CollUtil.isEmpty(tableMap)) {
                tableMap.forEach((tableName, tableInfos) -> {
                    IBaseHandler handler = handleMap.get(tableName);
                    if (Objects.nonNull(handler)) {
                        log.info("[执行Sink] 准备执行表名 [{}] 的Handler", tableName);
                        try {
                            HandlerContext handlerContext = HandlerContext.builder()
                                    .dbConfig(dbConfig)
                                    .outSqlSession(outSqlSession)
                                    .sinkSqlSession(sinkSqlSession)
                                    .tableInfos(tableInfos)
                                    .build();
                            handler.run(handlerContext);
                        } catch (Exception e) {
                            log.info("[执行Sink] 出现错误 ==> \n", e);
                        }
                    } else {
                        log.warn("[执行Sink] 表名 [{}] 未找到Handler", tableName);
                    }
                });
            }
        } catch (Exception e) {
            log.info("[执行Sink] CommonFlinkSink 执行出现错误 ==> \n", e);
        } finally {
            if (Objects.nonNull(outSqlSession)) {
                log.info("[执行Sink] 关闭源数据库SqlSession");
                outSqlSession.close();
            }
            if (Objects.nonNull(sinkSqlSession)) {
                log.info("[执行Sink] 关闭目标数据库SqlSession");
                sinkSqlSession.close();
            }
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
