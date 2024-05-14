package com.phaeris.flink.util;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.ResourceBundle;

import static com.phaeris.flink.constants.PropertiesConstants.PROPERTIES_NAME_FORMAT;

/**
 * @author wyh
 * @since 2024/5/14
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PropertiesUtil {

    public static ResourceBundle get(String env) {
        return ResourceBundle.getBundle(String.format(PROPERTIES_NAME_FORMAT, env));
    }
}
