package com.phaeris.flink.handler;

import cn.hutool.core.collection.CollUtil;
import com.phaeris.flink.enums.TableEnum;
import com.phaeris.flink.mapping.AbstractTableField;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * 通过serviceLoader获取同类service, 类似于{@code spring}的{@code @Autowired List<T> 或 @Autowired Map<String,T>}
 * <p>
 * 如果想便捷获取所有同类handler, 只需新建interface继承该类,
 * 然后所有同类handler实现新建的interface, 并在类上标记{@link com.google.auto.service.AutoService}即可.
 * <p>使用时直接{@link HandlerFactory#getHandlerMap(Class)}即可所有所有同类handler
 *
 * @author wyh
 * @since 2024/4/10
 */
public interface IBaseHandler extends Serializable {

    /**
     * 用户需要处理数据的地方
     *
     * @param context 上下文对象
     */
    void doProcess(HandlerContext context);

    /**
     * @return handler所属源表
     */
    List<TableEnum> froms();

    /**
     * 执行数据处理程序
     *
     * @param context 上下文对象
     */
    default void run(HandlerContext context) {
        if (Objects.isNull(context)) {
            return;
        }
        List<? extends AbstractTableField> tableInfos = context.getTableInfos();
        if (CollUtil.isEmpty(tableInfos) || context.getOutSqlSession() == null
                || context.getSinkSqlSession() == null) {
            return;
        }
        doProcess(context);
    }
}
