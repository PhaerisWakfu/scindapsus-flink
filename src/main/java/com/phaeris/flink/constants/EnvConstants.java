package com.phaeris.flink.constants;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * @author wyh
 * @since 2024/4/17
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class EnvConstants {

    public static final String ENV = "env";

    public static final String ODS_V1 = "ods.v1";

    public static final String TASK_VERSION = "version";

    public static final String DEFAULT_TASK_VERSION = "v1";

    public static final String STARTUP_MODEL = "mode";
}
