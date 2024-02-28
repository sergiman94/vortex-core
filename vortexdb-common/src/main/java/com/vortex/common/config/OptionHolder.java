package com.vortex.common.config;

import com.vortex.common.util.Log;
import org.slf4j.Logger;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class OptionHolder {

    private static final Logger LOG = Log.logger(VortexConfig.class);

    protected Map<String, TypedOption<?,?>> options;

    public OptionHolder() {this.options = new HashMap<>();}

    protected void registerOptions() {
        for (Field field  : this.getClass().getFields()) {
            if (!TypedOption.class.isAssignableFrom(field.getType())) {
                // skip if not option
                continue;
            }

            try {
                TypedOption<?, ?> option = (TypedOption<?, ?>) field.get(this);
                // Fields of subclass first, don't overwrite by superclass
                this.options.putIfAbsent(option.name(), option);
            } catch (Exception e ) {
                LOG.error("Failed to register option: {}", field, e);
                throw new ConfigException("Failed to register option: %s", field);
            }
        }
    }

    public Map<String, TypedOption<?,?>> options() {return Collections.unmodifiableMap(this.options);}

}
