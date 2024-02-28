package com.vortex.common.config;

import com.vortex.common.util.E;
import com.vortex.common.util.Log;
import org.apache.commons.configuration.*;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.units.qual.C;
import org.slf4j.Logger;

import javax.annotation.Nullable;
import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class VortexConfig extends org.apache.commons.configuration.PropertiesConfiguration {

    private static final Logger LOG = Log.logger(VortexConfig.class);

    public VortexConfig(Configuration config) {
        if (config == null) {
            throw new ConfigException("The config object is null");
        }
        this.reloadIfNeed(config);
        this.setLayoutIfNeeded(config);

        Iterator<String> keys = config.getKeys();
        while (keys.hasNext()) {
            String key = keys.next();
            this.addProperty(key, config.getProperty(key));
        }
        this.checkRequiredOptions();
    }

    public VortexConfig(String configFile) {
        this(loadConfigFile(configFile));
    }

    private void reloadIfNeed(Configuration conf) {
        if (!(conf instanceof AbstractFileConfiguration)) {
            if (conf instanceof AbstractConfiguration) {
                AbstractConfiguration config = (AbstractConfiguration) conf;
                config.setDelimiterParsingDisabled(true);
            }
            return;
        }
        AbstractFileConfiguration fileConfig = (AbstractFileConfiguration) conf;

        File file = fileConfig.getFile();
        if (file != null) {
            // May need to use the original file
            this.setFile(file);
        }

        if (!fileConfig.isDelimiterParsingDisabled()) {
            /*
             * PropertiesConfiguration will parse the containing comma
             * config options into list directly, but we want to do
             * this work by ourselves, so reload it and parse into `String`
             */
            fileConfig.setDelimiterParsingDisabled(true);
            try {
                fileConfig.refresh();
            } catch (org.apache.commons.configuration.ConfigurationException e) {
                throw new ConfigException("Unable to load config file: %s",
                        e, file);
            }
        }
    }

    private void setLayoutIfNeeded(Configuration conf) {
        if (!(conf instanceof org.apache.commons.configuration.PropertiesConfiguration)) {
            return;
        }
        org.apache.commons.configuration.PropertiesConfiguration propConf = (org.apache.commons.configuration.PropertiesConfiguration) conf;
        this.setLayout(propConf.getLayout());
    }

    private static org.apache.commons.configuration.PropertiesConfiguration loadConfigFile(String path) {
        E.checkNotNull(path, "config path");
        E.checkArgument(!path.isEmpty(),
                "The config path can't be empty");

        File file = new File(path);
        E.checkArgument(file.exists() && file.isFile() && file.canRead(),
                "Need to specify a readable config, but got: %s",
                file.toString());

        org.apache.commons.configuration.PropertiesConfiguration config = new PropertiesConfiguration();
        config.setDelimiterParsingDisabled(true);
        try {
            config.load(file);
        } catch (ConfigurationException e) {
            throw new ConfigException("Unable to load config: %s", e, path);
        }
        return config;
    }

    @SuppressWarnings("unchecked")
    public <T, R> R get(TypedOption<T, R> option) {
        Object value = this.getProperty(option.name());
        return value != null ? (R) value : option.defaultValue();
    }

    public Map<String, String> getMap(ConfigListOption<String> option) {
        List<String> values = this.get(option);
        Map<String, String> result = new HashMap<>();
        for (String value : values) {
            String[] pair = value.split(":", 2);
            E.checkState(pair.length == 2,
                    "Invalid option format for '%s': %s(expect KEY:VALUE)",
                    option.name(), value);
            result.put(pair[0].trim(), pair[1].trim());
        }
        return result;
    }

    @Override
    public void addProperty(String key, Object value) {

        if (key.startsWith("\"") && key.endsWith("\"")) {
            String newKey = key.replace("\"", "");

            if (!OptionSpace.containKey(newKey)) {
                LOG.warn("The config option '{}' is redundant, " +
                        "please ensure it has been registered", newKey);
            } else {
                // The input value is String(parsed by PropertiesConfiguration)
                value = this.validateOption(newKey, value);
            }
            super.addPropertyDirect(newKey, value);

        } else {
            if (!OptionSpace.containKey(key)) {
                LOG.warn("The config option '{}' is redundant, " +
                        "please ensure it has been registered", key);
            } else {
                // The input value is String(parsed by PropertiesConfiguration)
                value = this.validateOption(key, value);
            }
            super.addPropertyDirect(key, value);
        }


    }

    private Object validateOption(String key, Object value) {
        E.checkArgument(value instanceof String,
                "Invalid value for key '%s': %s", key, value);

        TypedOption<?, ?> option = OptionSpace.get(key);
        return option.parsedConvert((String) value);
    }

    private void checkRequiredOptions() {
        // TODO: Check required options must be contained in this map
    }



}
