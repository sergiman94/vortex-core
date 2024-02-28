
package com.vortex.distr;

import com.vortex.vortexdb.VortexException;
import com.vortex.vortexdb.VortexFactory;
import com.vortex.common.config.VortexConfig;
import com.vortex.api.config.ServerOptions;
import com.vortex.common.event.EventHub;
import com.vortex.api.server.RestServer;
import com.vortex.vortexdb.util.ConfigUtil;
import com.vortex.common.util.Log;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.slf4j.Logger;

public class VortexServer {

    private static final Logger LOG = Log.logger(VortexServer.class);

    private final GremlinServer gremlinServer;
    private final RestServer restServer;

    public static void register() {
        RegisterUtil.registerBackends();
        RegisterUtil.registerPlugins();
        RegisterUtil.registerServer();
    }

    public VortexServer(String gremlinServerConf, String restServerConf)
                           throws Exception {
        // Only switch on security manager after VortexGremlinServer started
        SecurityManager securityManager = System.getSecurityManager();
        System.setSecurityManager(null);

        ConfigUtil.checkGremlinConfig(gremlinServerConf);
        VortexConfig restServerConfig = new VortexConfig(restServerConf);
        String graphsDir = restServerConfig.get(ServerOptions.GRAPHS);
        EventHub hub = new EventHub("gremlin=>hub<=rest");
        try {
            // Start GremlinServer
            this.gremlinServer = VortexGremlinServer.start(gremlinServerConf,
                                                         graphsDir, hub);
        } catch (Throwable e) {
            LOG.error("VortexGremlinServer start error: ", e);
            VortexFactory.shutdown(30L);
            throw e;
        } finally {
            System.setSecurityManager(securityManager);
        }

        try {
            // Start VortexRestServer
            this.restServer = VortexRestServer.start(restServerConf, hub);
        } catch (Throwable e) {
            LOG.error("VortexRestServer start error: ", e);
            try {
                this.gremlinServer.stop().get();
            } catch (Throwable t) {
                LOG.error("GremlinServer stop error: ", t);
            }
            VortexFactory.shutdown(30L);
            throw e;
        }
    }

    public void stop() {
        try {
            this.restServer.shutdown().get();
            LOG.info("VortexRestServer stopped");
        } catch (Throwable e) {
            LOG.error("VortexRestServer stop error: ", e);
        }

        try {
            this.gremlinServer.stop().get();
            LOG.info("VortexGremlinServer stopped");
        } catch (Throwable e) {
            LOG.error("VortexGremlinServer stop error: ", e);
        }

        try {
            VortexFactory.shutdown(30L);
            LOG.info("Vortex stopped");
        } catch (Throwable e) {
            LOG.error("Failed to stop Vortex: ", e);
        }
    }

    public static void main(String[] args) throws Exception {
        LOG.info(String.valueOf(args.length));
        if (args.length != 2) {
            String msg = "Start VortexServer need to pass 2 parameters, " +
                         "they are the config files of GremlinServer and " +
                         "RestServer, for example: conf/gremlin-server.yaml " +
                         "conf/rest-server.properties";
            LOG.error(msg);
            throw new VortexException(msg);
        }

        VortexServer.register();

        VortexServer server = new VortexServer(args[0], args[1]);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("VortexServer stopping");
            server.stop();
            LOG.info("VortexServer stopped");
        }, "vortex-server-shutdown"));
    }
}
