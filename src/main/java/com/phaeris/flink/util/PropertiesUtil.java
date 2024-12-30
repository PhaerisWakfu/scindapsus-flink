package com.phaeris.flink.util;

import cn.hutool.core.io.resource.ResourceUtil;
import com.phaeris.flink.constants.PropertiesConstants;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;

import static com.phaeris.flink.constants.PropertiesConstants.PROPERTIES_SUFFIX;

/**
 * @author wyh
 * @since 2024/5/14
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PropertiesUtil {

    public static ParameterTool create(String env) {
        try {
            return ParameterTool.fromPropertiesFile(ResourceUtil.getResource(getConfigPath(env)).getPath());
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
        return ParameterTool.fromSystemProperties();
    }

    private static String getConfigPath(String env) {
        return String.format(PropertiesConstants.PROPERTIES_NAME_FORMAT, env) + PROPERTIES_SUFFIX;
    }
}
