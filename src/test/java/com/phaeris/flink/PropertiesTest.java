package com.phaeris.flink;

import com.phaeris.flink.util.PropertiesUtil;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * @author wyh
 * @since 2024/12/30
 */
public class PropertiesTest {

    public static void main(String[] args) {
        System.setProperty("hello", "world");
        ParameterTool parameterTool = PropertiesUtil.create("test");
        System.out.println(parameterTool.get("jdbc.mapper.locations"));
    }
}
