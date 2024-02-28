
package com.vortex.distr;

import com.vortex.common.config.ConfigListOption;
import com.vortex.common.config.OptionHolder;

import static com.vortex.common.config.OptionChecker.disallowEmpty;

public class DistOptions extends OptionHolder {

    private DistOptions() {
        super();
    }

    private static volatile DistOptions instance;

    public static synchronized DistOptions instance() {
        if (instance == null) {
            instance = new DistOptions();
            instance.registerOptions();
        }
        return instance;
    }

    public static final ConfigListOption<String> BACKENDS =
            new ConfigListOption<>(
                    "backends",
                    "The all data store type.",
                    disallowEmpty(),
                    "memory"
            );
}
