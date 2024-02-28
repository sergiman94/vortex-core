
package com.vortex.vortexdb;

import java.io.File;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;

import com.vortex.vortexdb.config.CoreOptions;
import com.vortex.common.config.VortexConfig;
import com.vortex.common.event.EventHub;
import com.vortex.vortexdb.task.TaskManager;
import com.vortex.vortexdb.traversal.algorithm.OltpTraverser;
import com.vortex.vortexdb.type.define.SerialEnum;
import com.vortex.common.util.E;
import com.vortex.common.util.Log;


public class VortexFactory {

    private static final Logger LOG = Log.logger(Vortex.class);

    static {
        SerialEnum.registerInternalEnums();
        Vortex.registerTraversalStrategies(StandardVortex.class);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Vortex is shutting down");
            VortexFactory.shutdown(30L);
        }, "Vortex-shutdown"));
    }

    private static final String NAME_REGEX = "^[A-Za-z][A-Za-z0-9_]{0,47}$";

    private static final Map<String, Vortex> graphs = new HashMap<>();

    public static synchronized Vortex open(Configuration config) {
        VortexConfig conf = config instanceof VortexConfig ?
                          (VortexConfig) config : new VortexConfig(config);
        return open(conf);
    }

    public static synchronized Vortex open(VortexConfig config) {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            // Not allowed to read file via Gremlin when SecurityManager enabled
            String configFile = config.getFileName();
            if (configFile == null) {
                configFile = config.toString();
            }
            sm.checkRead(configFile);
        }

        String name = config.get(CoreOptions.STORE);
        checkGraphName(name, "graph config(like vortex.properties)");
        name = name.toLowerCase();
        Vortex graph = graphs.get(name);
        if (graph == null || graph.closed()) {
            graph = new StandardVortex(config);
            graphs.put(name, graph);
        } else {
            String backend = config.get(CoreOptions.BACKEND);
            E.checkState(backend.equalsIgnoreCase(graph.backend()),
                         "Graph name '%s' has been used by backend '%s'",
                         name, graph.backend());
        }
        return graph;
    }

    public static Vortex open(String path) {
        return open(getLocalConfig(path));
    }

    public static Vortex open(URL url) {
        return open(getRemoteConfig(url));
    }

    public static void remove(Vortex graph) {
        String name = graph.option(CoreOptions.STORE);
        graphs.remove(name);
    }

    public static void checkGraphName(String name, String configFile) {
        E.checkArgument(name.matches(NAME_REGEX),
                        "Invalid graph name '%s' in %s, " +
                        "valid graph name is up to 48 alpha-numeric " +
                        "characters and underscores and only letters are " +
                        "supported as first letter. " +
                        "Note: letter is case insensitive", name, configFile);
    }

    public static PropertiesConfiguration getLocalConfig(String path) {
        File file = new File(path);
        E.checkArgument(file.exists() && file.isFile() && file.canRead(),
                        "Please specify a proper config file rather than: %s",
                        file.toString());
        try {
            return new PropertiesConfiguration(file);
        } catch (ConfigurationException e) {
            throw new VortexException("Unable to load config file: %s", e, path);
        }
    }

    public static PropertiesConfiguration getRemoteConfig(URL url) {
        try {
            return new PropertiesConfiguration(url);
        } catch (ConfigurationException e) {
            throw new VortexException("Unable to load remote config file: %s",
                                    e, url);
        }
    }

    /**
     * Stop all the daemon threads
     * @param timeout seconds
     */
    public static void shutdown(long timeout) {
        try {
            if (!EventHub.destroy(timeout)) {
                throw new TimeoutException(timeout + "s");
            }
            TaskManager.instance().shutdown(timeout);
            OltpTraverser.destroy();
        } catch (Throwable e) {
            LOG.error("Error while shutdown", e);
            throw new VortexException("Failed to shutdown", e);
        }
    }
}
