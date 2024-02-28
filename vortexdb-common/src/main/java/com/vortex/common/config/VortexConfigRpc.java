package com.vortex.common.config;

import com.vortex.common.util.Log;
import com.vortex.common.util.E;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.YAMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.io.FileHandler;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import javax.annotation.Nullable;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VortexConfigRpc extends PropertiesConfiguration {

    private static final Logger LOG = Log.logger(VortexConfigRpc.class);

    private final String path;

    // implemented
    public VortexConfigRpc(Configuration config) {
        loadConfig(config);
        this.path = null;
    }

    // implemented
    public VortexConfigRpc(String configFile) {
        loadConfig(loadConfigFile(configFile));
        this.path = configFile;
    }

    // implemented
    private void loadConfig(Configuration config) {
        if (config == null) {
            throw new ConfigException("The config object is null");
        }
        this.setLayoutIfNeeded(config);

        this.append(config);
        this.checkRequiredOptions();
    }

    // implemented
    private void setLayoutIfNeeded(Configuration conf) {
        if (!(conf instanceof PropertiesConfiguration)) {
            return;
        }
        PropertiesConfiguration propConf = (PropertiesConfiguration) conf;
        this.setLayout(propConf.getLayout());
    }

    // implemented
    private static Configuration loadConfigFile(String path) {
        E.checkNotNull(path, "config path");
        E.checkArgument(!path.isEmpty(),
                "The config path can't be empty");

        File file = new File(path);
        return loadConfigFile(file);
    }

    // implemented
    @SuppressWarnings("unchecked")
    public <T, R> R get(TypedOption<T, R> option) {
        Object value = this.getProperty(option.name());
        if (value == null) {
            return option.defaultValue();
        }
        return (R) value;
    }

    // implmented
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

    // implemented
    @Override
    public void addPropertyDirect(String key, Object value) {
        TypedOption<?, ?> option = OptionSpace.get(key);
        if (option == null) {
            LOG.warn("The config option '{}' is redundant, " +
                    "please ensure it has been registered", key);
        } else {
            // The input value is String(parsed by PropertiesConfiguration)
            value = this.validateOption(key, value);
        }
        if (this.containsKey(key) && value instanceof List) {
            for (Object item : (List<Object>) value) {
                super.addPropertyDirect(key, item);
            }
        } else {
            super.addPropertyDirect(key, value);
        }
    }

    // implemented
    @Override
    protected void addPropertyInternal(String key, Object value) {
        this.addPropertyDirect(key, value);
    }

    // implemented
    private Object validateOption(String key, Object value) {
        TypedOption<?, ?> option = OptionSpace.get(key);

        if (value instanceof String) {
            return option.parsedConvert((String) value);
        }

        Class dataType = option.dataType();
        if (dataType.isInstance(value)) {
            return value;
        }

        throw new IllegalArgumentException(
                String.format("Invalid value for key '%s': '%s'", key, value));
    }

    // implemented
    private void checkRequiredOptions() {
        // TODO: Check required options must be contained in this map
    }

    // implemented
    public void save(File copiedFile) throws ConfigurationException {
        FileHandler fileHandler = new FileHandler(this);
        fileHandler.save(copiedFile);
    }

    // implemented
    @Nullable
    public File getFile() {
        if (StringUtils.isEmpty(this.path)) {
            return null;
        }

        return new File(this.path);
    }

    // implemented
    private static Configuration loadConfigFile(File configFile) {
        E.checkArgument(configFile.exists() &&
                        configFile.isFile() &&
                        configFile.canRead(),
                "Please specify a proper config file rather than: '%s'",
                configFile.toString());

        try {
            String fileName = configFile.getName();
            String fileExtension = FilenameUtils.getExtension(fileName);

            Configuration config;
            Configurations configs = new Configurations();

            switch (fileExtension) {
                case "yml":
                case "yaml":
                    Parameters params = new Parameters();
                    FileBasedConfigurationBuilder<FileBasedConfiguration>
                            builder = new FileBasedConfigurationBuilder(
                            YAMLConfiguration.class)
                            .configure(params.fileBased()
                                    .setFile(configFile));
                    config = builder.getConfiguration();
                    break;
                case "xml":
                    config = configs.xml(configFile);
                    break;
                default:
                    config = configs.properties(configFile);
                    break;
            }
            return config;
        } catch (ConfigurationException e) {
            throw new ConfigException("Unable to load config: '%s'",
                    e, configFile);
        }
    }
}
