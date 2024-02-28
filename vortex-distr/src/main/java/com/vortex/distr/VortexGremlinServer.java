
package com.vortex.distr;

import com.vortex.vortexdb.VortexException;
import com.vortex.api.auth.ContextGremlinServer;
import com.vortex.common.event.EventHub;
import com.vortex.vortexdb.util.ConfigUtil;
import com.vortex.common.util.E;
import com.vortex.common.util.Log;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.slf4j.Logger;

public class VortexGremlinServer {

    private static final Logger LOG = Log.logger(VortexGremlinServer.class);

    public static GremlinServer start(String conf, String graphsDir,
                                      EventHub hub) throws Exception {
        // Start GremlinServer with inject traversal source
        String gremlinHeader = GremlinServer.getHeader();
        LOG.info("Starting gremlin server ... ");
        LOG.info(gremlinHeader);
        final Settings settings;
        LOG.info("Settings instantiated");
        try {
            settings = Settings.read(conf);
        } catch (Exception e) {
            LOG.error("Can't found the configuration file at '{}' or " +
                      "being parsed properly. [{}]", conf, e.getMessage());
            throw e;
        }
        // Scan graph confs and inject into gremlin server context
        E.checkState(settings.graphs != null,
                     "The GremlinServer's settings.graphs is null");
        settings.graphs.putAll(ConfigUtil.scanGraphsDir(graphsDir));

        LOG.info("Configuring Gremlin Server from {}", conf);
        ContextGremlinServer server = new ContextGremlinServer(settings, hub);

        // Inject customized traversal source
        server.injectTraversalSource();

        server.start().exceptionally(t -> {
            LOG.error("Gremlin Server was unable to start and will " +
                      "shutdown now: {}", t.getMessage());
            server.stop().join();
            throw new VortexException("Failed to start Gremlin Server");
        }).join();

        return server;
    }
}
