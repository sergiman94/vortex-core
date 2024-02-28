
package com.vortex.vortexdb.util;

import com.vortex.common.util.InsertionOrderUtil;
import com.vortex.common.util.Log;
import com.vortex.vortexdb.VortexException;
import com.vortex.vortexdb.VortexFactory;
import com.vortex.common.config.VortexConfig;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.tree.ConfigurationNode;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.util.config.YamlConfiguration;
import com.vortex.common.util.E;
import org.slf4j.Logger;

import java.io.*;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

public final class ConfigUtil {

    private static final Logger LOG = Log.logger(ConfigUtil.class);

    private static final String NODE_GRAPHS = "graphs";
    private static final String CONF_SUFFIX = ".properties";
    private static final String CHARSET = "UTF-8";

    public static void checkGremlinConfig(String conf) {
        YamlConfiguration yamlConfig = new YamlConfiguration();
        try {
            yamlConfig.load(conf);
        } catch (ConfigurationException e) {
            throw new VortexException("Failed to load yaml config file '%s'",
                                    conf);
        }
        List<ConfigurationNode> nodes = yamlConfig.getRootNode()
                                                  .getChildren(NODE_GRAPHS);
        E.checkArgument(nodes == null || nodes.size() == 1,
                        "Not allowed to specify multiple '%s' nodes in " +
                        "config file '%s'", NODE_GRAPHS, conf);
        if (nodes != null) {
            List<ConfigurationNode> graphNames = nodes.get(0).getChildren();
            E.checkArgument(graphNames.isEmpty(),
                            "Don't allow to fill value for '%s' node in " +
                            "config file '%s'", NODE_GRAPHS, conf);
        }
    }

    public static Map<String, String> scanGraphsDir(String graphsDirPath) {
        LOG.info("Scaning option 'graphs' directory '{}'", graphsDirPath);
        File graphsDir = new File(graphsDirPath);
        E.checkArgument(graphsDir.exists() && graphsDir.isDirectory(),
                        "Please ensure the path '%s' of option 'graphs' " +
                        "exist and it's a directory", graphsDir);
        File[] confFiles = graphsDir.listFiles((dir, name) -> {
            return name.endsWith(CONF_SUFFIX);
        });
        E.checkNotNull(confFiles, "graph configuration files");
        Map<String, String> graphConfs = InsertionOrderUtil.newMap();
        for (File confFile : confFiles) {
            // NOTE: use file name as graph name
            String name = StringUtils.substringBefore(confFile.getName(),
                                                      ConfigUtil.CONF_SUFFIX);
            VortexFactory.checkGraphName(name, confFile.getPath());
            graphConfs.put(name, confFile.getPath());
        }
        return graphConfs;
    }

    public static void writeToFile(String dir, String graphName,
                                   VortexConfig config) {
        E.checkArgument(FileUtils.getFile(dir).exists(),
                        "The directory '%s' must exist", dir);
        String fileName = Paths.get(dir, graphName + CONF_SUFFIX).toString();
        try (OutputStream os = new FileOutputStream(fileName)) {
            config.save(os, CHARSET);
            config.setFileName(fileName);
            LOG.info("Write VortexConfig to file: '{}'", fileName);
        } catch (IOException | ConfigurationException e) {
            throw new VortexException("Failed to write VortexConfig to file '%s'",
                                    e, fileName);
        }
    }

    public static void deleteFile(File file) {
        if (!file.exists()) {
            return;
        }
        try {
            FileUtils.forceDelete(file);
        } catch (IOException e) {
            throw new VortexException("Failed to delete VortexConfig file '%s'",
                                    e, file);
        }
    }

    public static PropertiesConfiguration buildConfig(String configText) {
        E.checkArgument(StringUtils.isNotEmpty(configText),
                        "The config text can't be null or empty");
        PropertiesConfiguration propConfig = new PropertiesConfiguration();
        try {
            InputStream in = new ByteArrayInputStream(configText.getBytes(
                                                      CHARSET));
            propConfig.setDelimiterParsingDisabled(true);
            propConfig.load(in);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read config options", e);
        }
        return propConfig;
    }
}
