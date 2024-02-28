package com.vortex.distr.cmd;

import com.vortex.vortexdb.VortexFactory;
import com.vortex.vortexdb.Vortex;
import com.vortex.api.auth.StandardAuthenticator;
import com.vortex.vortexdb.backend.store.BackendStoreSystemInfo;
import com.vortex.vortexdb.config.CoreOptions;
import com.vortex.common.config.VortexConfig;
import com.vortex.api.config.ServerOptions;
import com.vortex.distr.RegisterUtil;
import com.vortex.vortexdb.util.ConfigUtil;
import com.vortex.common.util.E;
import com.vortex.common.util.Log;
import org.apache.commons.collections.map.MultiValueMap;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Map;

public class InitStore {

    private static final Logger LOG = Log.logger(InitStore.class);

    // 6~8 retries may be needed under high load for Cassandra backend
    private static final int RETRIES = 10;
    // Less than 5000 may cause mismatch exception with Cassandra backend
    private static final long RETRY_INTERVAL = 5000;

    private static final MultiValueMap exceptions = new MultiValueMap();

    static {
        exceptions.put("OperationTimedOutException",
                       "Timed out waiting for server response");
        exceptions.put("NoHostAvailableException",
                       "All host(s) tried for query failed");
        exceptions.put("InvalidQueryException", "does not exist");
        exceptions.put("InvalidQueryException", "unconfigured table");
    }

    public static void main(String[] args) throws Exception {
        E.checkArgument(args.length == 1,
                        "Vortex init-store need to pass the config file " +
                        "of RestServer, like: conf/rest-server.properties");
        E.checkArgument(args[0].endsWith(".properties"),
                        "Expect the parameter is properties config file.");

        String restConf = args[0];

        RegisterUtil.registerBackends();
        RegisterUtil.registerPlugins();
        RegisterUtil.registerServer();

        VortexConfig restServerConfig = new VortexConfig(restConf);
        String graphsDir = restServerConfig.get(ServerOptions.GRAPHS);
        Map<String, String> graphs = ConfigUtil.scanGraphsDir(graphsDir);

        for (Map.Entry<String, String> entry : graphs.entrySet()) {
            initGraph(entry.getValue());
        }

        StandardAuthenticator.initAdminUserIfNeeded(restConf);

        VortexFactory.shutdown(30L);
    }

    private static void initGraph(String configPath) throws Exception {
        LOG.info("Init graph with config file: {}", configPath);
        VortexConfig config = new VortexConfig(configPath);
        // Forced set RAFT_MODE to false when initializing backend
        config.setProperty(CoreOptions.RAFT_MODE.name(), "false");
        Vortex graph = (Vortex) GraphFactory.open(config);

        BackendStoreSystemInfo sysInfo = graph.backendStoreSystemInfo();
        try {
            if (sysInfo.exists()) {
                LOG.info("Skip init-store due to the backend store of '{}' " +
                         "had been initialized", graph.name());
                sysInfo.checkVersion();
            } else {
                initBackend(graph);
            }
        } finally {
            graph.close();
        }
    }

    private static void initBackend(final Vortex graph)
                                    throws InterruptedException {
        int retries = RETRIES;
        retry: do {
            try {
                graph.initBackend();
            } catch (Exception e) {
                String clz = e.getClass().getSimpleName();
                String message = e.getMessage();
                if (exceptions.containsKey(clz) && retries > 0) {
                    @SuppressWarnings("unchecked")
                    Collection<String> keywords = exceptions.getCollection(clz);
                    for (String keyword : keywords) {
                        if (message.contains(keyword)) {
                            LOG.info("Init failed with exception '{} : {}', " +
                                     "retry  {}...",
                                     clz, message, RETRIES - retries + 1);

                            Thread.sleep(RETRY_INTERVAL);
                            continue retry;
                        }
                    }
                }
                throw e;
            }
            break;
        } while (retries-- > 0);
    }
}
