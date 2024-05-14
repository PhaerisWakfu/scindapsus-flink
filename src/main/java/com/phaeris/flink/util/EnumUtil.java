package com.phaeris.flink.util;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * @author wyh
 * @since 2023/2/13
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class EnumUtil {

    /**
     * 根据枚举中的某个字段的值获取该枚举
     * <p>
     * 如该枚举字段值不是唯一的返回第一个匹配的枚举
     *
     * @param clazz 枚举类型
     * @param value 枚举某个字段的值
     * @param getV  获取是枚举的哪个字段
     * @param def   默认枚举
     * @param <T>   枚举类型
     * @param <K>   字段值类型
     * @return 获取到的枚举
     */
    public static <T extends Enum<?>, K> T valueOf(Class<T> clazz, K value, Function<T, K> getV, T def) {
        return valueOf(clazz, value, getV)
                .orElseGet(() -> {
                    log.warn("enum value [{}] not found, use default value[{}]", value, def);
                    return def;
                });
    }

    /**
     * 根据枚举中的某个字段的值获取该枚举
     * <p>
     * 如该枚举字段值不是唯一的返回第一个匹配的枚举
     *
     * @param clazz             枚举类型
     * @param value             枚举某个字段的值
     * @param getV              获取是枚举的哪个字段
     * @param exceptionSupplier 找不到抛异常
     * @param <T>               枚举类型
     * @param <K>               字段值类型
     * @param <X>               异常类型
     * @return 获取到的枚举
     */
    public static <T extends Enum<?>, K, X extends Throwable> T valueOfThrow(Class<T> clazz, K value, Function<T, K> getV,
                                                                             Supplier<? extends X> exceptionSupplier) throws X {
        return valueOf(clazz, value, getV).orElseThrow(exceptionSupplier);
    }

    private static <T extends Enum<?>, K> Optional<T> valueOf(Class<T> clazz, K value, Function<T, K> getV) {
        T[] enums = clazz.getEnumConstants();
        return Stream.of(enums)
                .filter(x -> Objects.equals(value, getV.apply(x)))
                .findFirst();
    }
}
