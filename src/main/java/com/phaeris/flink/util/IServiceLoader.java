package com.phaeris.flink.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.function.Function;

/**
 * @author wyh
 * @since 2024/3/18
 */
public class IServiceLoader {

    /**
     * 根据特征获取class具体实例
     *
     * @param clazz 需要获取的实例class
     * @param func  实例的特征方法
     * @param val   需要匹配特征的值
     * @param <T>   实例类型
     * @param <K>   特征类型
     * @return 实例对象
     */
    public static <T, K> T find(Class<T> clazz, Function<T, K> func, K val) {
        for (T ele : ServiceLoader.load(clazz)) {
            if (Objects.equals(func.apply(ele), val)) {
                return ele;
            }
        }
        return null;
    }

    /**
     * 获取class的所有实例
     *
     * @param clazz 需要获取的实例class
     * @param <T>   实例类型
     * @return 实例对象list
     */
    public static <T> List<T> find(Class<T> clazz) {
        List<T> result = new ArrayList<>();
        for (T ele : ServiceLoader.load(clazz)) {
            result.add(ele);
        }
        return result;
    }
}
