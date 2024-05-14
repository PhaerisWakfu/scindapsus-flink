package com.phaeris.flink.agg;

import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @author wyh
 * @since 2024/4/17
 */
public class ListAgg<IN> implements AggregateFunction<IN, List<IN>, List<IN>> {

    @Override
    public List<IN> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public List<IN> add(IN entity, List<IN> list) {
        list.add(entity);
        return list;
    }

    @Override
    public List<IN> getResult(List<IN> list) {
        return list;
    }

    @Override
    public List<IN> merge(List<IN> entityList, List<IN> acc1) {
        acc1.addAll(entityList);
        return acc1;
    }
}
