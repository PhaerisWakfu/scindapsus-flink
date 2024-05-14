package com.phaeris.flink.handler;

import com.phaeris.flink.enums.TableEnum;
import com.phaeris.flink.util.IServiceLoader;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author wyh
 * @since 2024/4/10
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class HandlerFactory {

    public static <K, T extends IBaseHandler> Map<String, IBaseHandler> getHandlerMap(Class<T> clazz) {
        List<T> handlers = IServiceLoader.find(clazz);
        Map<List<String>, T> multiKeyHandlerMap = handlers.stream()
                .collect(Collectors.toMap(h -> h.froms().stream()
                                .map(TableEnum::getName)
                                .collect(Collectors.toList()),
                        h -> h));
        Map<String, IBaseHandler> result = new HashMap<>();
        for (Map.Entry<List<String>, T> entry : multiKeyHandlerMap.entrySet()) {
            entry.getKey().forEach(k -> result.put(k, entry.getValue()));
        }
        return result;
    }
}
