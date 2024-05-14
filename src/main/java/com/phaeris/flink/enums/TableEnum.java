package com.phaeris.flink.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author wyh
 * @since 2024/4/16
 */
public interface TableEnum {

    String getName();

    @Getter
    @AllArgsConstructor
    enum ODS implements TableEnum {
        t_user("t_user", "用户表");
        private final String name;
        private final String comment;
    }

    @Getter
    @AllArgsConstructor
    enum ADS implements TableEnum {
        ads_user_income("ads_user_income", "用户收入表");
        private final String name;
        private final String comment;
    }
}
