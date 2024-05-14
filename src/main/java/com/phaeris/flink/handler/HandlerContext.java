package com.phaeris.flink.handler;

import com.phaeris.flink.config.DatasourceConfig;
import com.phaeris.flink.mapping.AbstractTableField;
import lombok.Builder;
import lombok.Data;
import org.apache.ibatis.session.SqlSession;

import java.io.Serializable;
import java.util.List;

/**
 * @author wyh
 * @since 2024/4/17
 */
@Data
@Builder
public class HandlerContext implements Serializable {

    private static final long serialVersionUID = -1219973981630059388L;

    private SqlSession outSqlSession;

    private SqlSession sinkSqlSession;

    private List<? extends AbstractTableField> tableInfos;

    private DatasourceConfig dbConfig;
}
