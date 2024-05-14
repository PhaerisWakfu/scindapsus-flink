package com.phaeris.flink.mapping.impl;

import com.phaeris.flink.mapping.AbstractTableField;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author wyh
 * @since 2024/4/16
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class UserTableField extends AbstractTableField {

    private Long id;

    public UserTableField(String table, Long id) {
        this.table = table;
        this.id = id;
    }
}
