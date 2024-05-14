package com.phaeris.flink.entity.po;

import lombok.Data;

/**
 * @author wyh
 * @since 2024/3/15
 */
@Data
public class User {

    private Long id;

    private String name;

    private String phone;

    private String address;
}
