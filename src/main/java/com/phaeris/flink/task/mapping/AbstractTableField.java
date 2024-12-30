package com.phaeris.flink.task.mapping;

import lombok.Data;

import java.io.Serializable;

/**
 * @author wyh
 * @since 2024/4/17
 */
@Data
public abstract class AbstractTableField implements Serializable {

    private static final long serialVersionUID = 1L;

    public String table;

    @SuppressWarnings("unchecked")
    public <T extends AbstractTableField> T convert() {
        return (T) this;
    }
}
