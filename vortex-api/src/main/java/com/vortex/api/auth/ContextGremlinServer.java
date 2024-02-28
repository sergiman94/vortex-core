
package com.vortex.api.auth;

import com.vortex.vortexdb.VortexException;
import com.vortex.vortexdb.Vortex;
import com.vortex.api.auth.VortexAuthProxy.Context;
import com.vortex.api.auth.VortexAuthProxy.ContextThreadPoolExecutor;
import com.vortex.vortexdb.config.CoreOptions;
import com.vortex.common.event.EventHub;
import com.vortex.common.testutil.Whitebox;
import com.vortex.vortexdb.util.Events;
import com.vortex.common.util.Log;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.util.ThreadFactoryUtil;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

/**
 * GremlinServer with custom ServerGremlinExecutor, which can pass Context
 */
public class ContextGremlinServer extends GremlinServer {

    private static final Logger LOG = Log.logger(GremlinServer.class);

    private static final String G_PREFIX = "__g_";

    private final EventHub eventHub;

    public ContextGremlinServer(final Settings settings, EventHub eventHub) {
        /*
         * pass custom Executor https://github.com/apache/tinkerpop/pull/813
         */
        super(settings, newGremlinExecutorService(settings));
        this.eventHub = eventHub;
        this.listenChanges();
    }

    private void listenChanges() {
        this.eventHub.listen(Events.GRAPH_CREATE, event -> {
            LOG.debug("GremlinServer accepts event '{}'", event.getName());
            event.checkArgs(Vortex.class);
            Vortex graph = (Vortex) event.getArgs()[0];
            this.injectGraph(graph);
            return null;
        });
        this.eventHub.listen(Events.GRAPH_DROP, event -> {
            LOG.debug("GremlinServer accepts event '{}'", event.getName());
            event.checkArgs(Vortex.class);
            Vortex graph = (Vortex) event.getArgs()[0];
            this.removeGraph(graph.name());
            return null;
        });
    }

    private void unlistenChanges() {
        this.eventHub.unlisten(Events.GRAPH_CREATE);
        this.eventHub.unlisten(Events.GRAPH_DROP);
    }

    @Override
    public synchronized CompletableFuture<Void> stop() {
        try {
            return super.stop();
        } finally {
            this.unlistenChanges();
        }
    }

    public void injectAuthGraph() {
        VortexAuthProxy.setContext(Context.admin());

        GraphManager manager = this.getServerGremlinExecutor()
                                   .getGraphManager();
        for (String name : manager.getGraphNames()) {
            Graph graph = manager.getGraph(name);
            graph = new VortexAuthProxy((Vortex) graph);
            manager.putGraph(name, graph);
        }
    }

    public void injectTraversalSource() {
        GraphManager manager = this.getServerGremlinExecutor()
                                   .getGraphManager();
        for (String graph : manager.getGraphNames()) {
            GraphTraversalSource g = manager.getGraph(graph).traversal();
            String gName = G_PREFIX + graph;
            if (manager.getTraversalSource(gName) != null) {
                throw new VortexException(
                          "Found existing name '%s' in global bindings, " +
                          "it may lead to gremlin query error.", gName);
            }
            // Add a traversal source for all graphs with customed rule.
            manager.putTraversalSource(gName, g);
        }
    }

    private void injectGraph(Vortex graph) {
        String name = graph.name();
        GraphManager manager = this.getServerGremlinExecutor()
                                   .getGraphManager();
        GremlinExecutor executor = this.getServerGremlinExecutor()
                                       .getGremlinExecutor();

        manager.putGraph(name, graph);

        GraphTraversalSource g = manager.getGraph(name).traversal();
        manager.putTraversalSource(G_PREFIX + name, g);

        Whitebox.invoke(executor, "globalBindings",
                        new Class<?>[]{ String.class, Object.class },
                        "put", name, graph);
    }

    private void removeGraph(String name) {
        GraphManager manager = this.getServerGremlinExecutor()
                                   .getGraphManager();
        GremlinExecutor executor = this.getServerGremlinExecutor()
                                       .getGremlinExecutor();
        try {
            manager.removeGraph(name);
            manager.removeTraversalSource(G_PREFIX + name);
            Whitebox.invoke(executor, "globalBindings",
                            new Class<?>[]{ Object.class },
                            "remove", name);
        } catch (Exception e) {
            throw new VortexException("Failed to remove graph '%s' from " +
                                    "gremlin server context", e, name);
        }
    }

    static ExecutorService newGremlinExecutorService(Settings settings) {
        if (settings.gremlinPool == 0) {
            settings.gremlinPool = CoreOptions.CPUS;
        }
        int size = settings.gremlinPool;
        ThreadFactory factory = ThreadFactoryUtil.create("exec-%d");
        return new ContextThreadPoolExecutor(size, size, factory);
    }
}
