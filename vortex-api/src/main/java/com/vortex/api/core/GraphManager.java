
package com.vortex.api.core;


import com.vortex.common.config.VortexConfig;
import com.vortex.rpc.rpc.RpcClientProvider;
import com.vortex.rpc.rpc.RpcProviderConfig;
import com.vortex.rpc.rpc.RpcServer;
import com.vortex.vortexdb.VortexFactory;
import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.auth.AuthManager;
import com.vortex.api.auth.VortexAuthenticator;
import com.vortex.api.auth.VortexFactoryAuthProxy;
import com.vortex.api.auth.VortexAuthProxy;
import com.vortex.vortexdb.backend.BackendException;
import com.vortex.vortexdb.backend.cache.Cache;
import com.vortex.vortexdb.backend.cache.CacheManager;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.id.IdGenerator;
import com.vortex.vortexdb.backend.store.BackendStoreSystemInfo;
import com.vortex.vortexdb.config.CoreOptions;

import com.vortex.api.config.ServerOptions;
import com.vortex.common.config.TypedOption;
import com.vortex.common.event.EventHub;
import com.vortex.vortexdb.exception.NotSupportException;
import com.vortex.api.license.LicenseVerifier;
import com.vortex.api.metrics.MetricsUtil;
import com.vortex.api.metrics.ServerReporter;

import com.vortex.api.serializer.JsonSerializer;
import com.vortex.api.serializer.Serializer;
import com.vortex.api.server.RestServer;
import com.vortex.vortexdb.rpc.RpcServiceConfig4Client;
import com.vortex.vortexdb.rpc.RpcServiceConfig4Server;
import com.vortex.vortexdb.task.TaskManager;
import com.vortex.vortexdb.type.define.NodeRole;
import com.vortex.vortexdb.util.ConfigUtil;
import com.vortex.common.util.E;
import com.vortex.vortexdb.util.Events;
import com.vortex.common.util.Log;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public final class GraphManager {

    private static final Logger LOG = Log.logger(RestServer.class);

    private final String graphsDir;
    private final Map<String, Graph> graphs;
    private final VortexAuthenticator authenticator;
    private final RpcServer rpcServer;
    private final RpcClientProvider rpcClient;

    private Id server;
    private NodeRole role;

    private final EventHub eventHub;

    public GraphManager(VortexConfig conf, EventHub hub) {
        this.graphsDir = conf.get(ServerOptions.GRAPHS);
        this.graphs = new ConcurrentHashMap<>();
        this.authenticator = VortexAuthenticator.loadAuthenticator(conf);
        this.rpcServer = new RpcServer(conf);
        this.rpcClient = new RpcClientProvider(conf);
        this.eventHub = hub;
        this.listenChanges();
        this.loadGraphs(ConfigUtil.scanGraphsDir(this.graphsDir));
        // this.installLicense(conf, "");
        // Raft will load snapshot firstly then launch election and replay log
        this.waitGraphsStarted();
        this.checkBackendVersionOrExit(conf);
        this.startRpcServer();
        this.serverStarted(conf);
        this.addMetrics(conf);
    }

    public void loadGraphs(final Map<String, String> graphConfs) {
        for (Map.Entry<String, String> conf : graphConfs.entrySet()) {
            String name = conf.getKey();
            String path = conf.getValue();
            VortexFactory.checkGraphName(name, "rest-server.properties");
            try {
                this.loadGraph(name, path);
            } catch (RuntimeException e) {
                LOG.error("Graph '{}' can't be loaded: '{}'", name, path, e);
            }
        }
    }

    public void waitGraphsStarted() {
        this.graphs.keySet().forEach(name -> {
            Vortex graph = this.graph(name);
            graph.waitStarted();
        });
    }

    public Vortex cloneGraph(String name, String newName,
                                String configText) {
        /*
         * 0. check and modify params
         * 1. create graph instance
         * 2. init backend store
         * 3. inject graph and traversal source into gremlin server context
         * 4. inject graph into rest server context
         */
        Vortex cloneGraph = this.graph(name);
        E.checkArgumentNotNull(cloneGraph,
                               "The clone graph '%s' doesn't exist", name);
        E.checkArgument(StringUtils.isNotEmpty(newName),
                        "The graph name can't be null or empty");
        E.checkArgument(!this.graphs().contains(newName),
                        "The graph '%s' has existed", newName);

        VortexConfig cloneConfig = cloneGraph.cloneConfig(newName);
        if (StringUtils.isNotEmpty(configText)) {
            PropertiesConfiguration propConfig = ConfigUtil.buildConfig(
                                                 configText);
            // Use the passed config to overwrite the old one
            propConfig.getKeys().forEachRemaining(key -> {
                cloneConfig.setProperty(key, propConfig.getProperty(key));
            });
            this.checkOptions(cloneConfig);
        }

        return this.createGraph(cloneConfig, newName);
    }

    public Vortex createGraph(String name, String configText) {
        E.checkArgument(StringUtils.isNotEmpty(name),
                        "The graph name can't be null or empty");
        E.checkArgument(!this.graphs().contains(name),
                        "The graph name '%s' has existed", name);

        PropertiesConfiguration propConfig = ConfigUtil.buildConfig(configText);
        VortexConfig config = new VortexConfig(propConfig);
        this.checkOptions(config);

        return this.createGraph(config, name);
    }

    public void dropGraph(String name) {
        Vortex graph = this.graph(name);
        E.checkArgumentNotNull(graph, "The graph '%s' doesn't exist", name);
        E.checkArgument(this.graphs.size() > 1,
                        "The graph '%s' is the only one, not allowed to delete",
                        name);

        this.dropGraph(graph);

        // Let gremlin server and rest server context remove graph
        this.notifyAndWaitEvent(Events.GRAPH_DROP, graph);
    }

    public Set<String> graphs() {
        return Collections.unmodifiableSet(this.graphs.keySet());
    }

    public Vortex graph(String name) {
        Graph graph = this.graphs.get(name);
        if (graph == null) {
            return null;
        } else if (graph instanceof Vortex) {
            return (Vortex) graph;
        }
        throw new NotSupportException("graph instance of %s", graph.getClass());
    }

    public Serializer serializer(Graph g) {
        return JsonSerializer.instance();
    }

    public void rollbackAll() {
        this.graphs.values().forEach(graph -> {
            if (graph.features().graph().supportsTransactions() &&
                graph.tx().isOpen()) {
                graph.tx().rollback();
            }
        });
    }

    public void rollback(final Set<String> graphSourceNamesToCloseTxOn) {
        closeTx(graphSourceNamesToCloseTxOn, Transaction.Status.ROLLBACK);
    }

    public void commitAll() {
        this.graphs.values().forEach(graph -> {
            if (graph.features().graph().supportsTransactions() &&
                graph.tx().isOpen()) {
                graph.tx().commit();
            }
        });
    }

    public void commit(final Set<String> graphSourceNamesToCloseTxOn) {
        closeTx(graphSourceNamesToCloseTxOn, Transaction.Status.COMMIT);
    }

    public boolean requireAuthentication() {
        if (this.authenticator == null) {
            return false;
        }
        return this.authenticator.requireAuthentication();
    }

    public VortexAuthenticator.User authenticate(Map<String, String> credentials)
                                               throws AuthenticationException {
        return this.authenticator().authenticate(credentials);
    }

    public AuthManager authManager() {
        return this.authenticator().authManager();
    }

    public void close() {
        this.destroyRpcServer();
        this.unlistenChanges();
    }

    private void startRpcServer() {
        if (!this.rpcServer.enabled()) {
            LOG.info("RpcServer is not enabled, skip starting rpc service");
            return;
        }

        RpcProviderConfig serverConfig = this.rpcServer.config();

        // Start auth rpc service if authenticator enabled
        if (this.authenticator != null) {
            serverConfig.addService(AuthManager.class,
                                    this.authenticator.authManager());
        }

        // Start graph rpc service if RPC_REMOTE_URL enabled
        if (this.rpcClient.enabled()) {
            RpcServiceConfig4Client clientConfig = (RpcServiceConfig4Client) this.rpcClient.config();

            for (Graph graph : this.graphs.values()) {
                Vortex vgraph = (Vortex) graph;
                vgraph.registerRpcServices((RpcServiceConfig4Server) serverConfig, clientConfig);
            }
        }

        try {
            this.rpcServer.exportAll();
        } catch (Throwable e) {
            this.rpcServer.destroy();
            throw e;
        }
    }

    private void destroyRpcServer() {
        try {
            this.rpcClient.destroy();
        } finally {
            this.rpcServer.destroy();
        }
    }

    private VortexAuthenticator authenticator() {
        E.checkState(this.authenticator != null,
                     "Unconfigured authenticator, please config " +
                     "auth.authenticator option in rest-server.properties");
        return this.authenticator;
    }

    @SuppressWarnings("unused")
    private void installLicense(VortexConfig config, String md5) {
        LicenseVerifier.instance().install(config, this, md5);
    }

    private void closeTx(final Set<String> graphSourceNamesToCloseTxOn,
                         final Transaction.Status tx) {
        final Set<Graph> graphsToCloseTxOn = new HashSet<>();

        graphSourceNamesToCloseTxOn.forEach(name -> {
            if (this.graphs.containsKey(name)) {
                graphsToCloseTxOn.add(this.graphs.get(name));
            }
        });

        graphsToCloseTxOn.forEach(graph -> {
            if (graph.features().graph().supportsTransactions() &&
                graph.tx().isOpen()) {
                if (tx == Transaction.Status.COMMIT) {
                    graph.tx().commit();
                } else {
                    graph.tx().rollback();
                }
            }
        });
    }

    private void loadGraph(String name, String path) {
        final Graph graph = GraphFactory.open(path);
        this.graphs.put(name, graph);
        LOG.info("Graph '{}' was successfully configured via '{}'", name, path);

        if (this.requireAuthentication() &&
            !(graph instanceof VortexAuthProxy)) {
            LOG.warn("You may need to support access control for '{}' with {}",
                     path, VortexFactoryAuthProxy.GRAPH_FACTORY);
        }
    }

    private void checkBackendVersionOrExit(VortexConfig config) {
        for (String graph : this.graphs()) {
            // TODO: close tx from main thread
            Vortex vgraph = this.graph(graph);
            if (!vgraph.backendStoreFeatures().supportsPersistence()) {
                vgraph.initBackend();
                if (this.requireAuthentication()) {
                    String token = config.get(ServerOptions.AUTH_ADMIN_TOKEN);
                    try {
                        this.authenticator.initAdminUser(token);
                    } catch (Exception e) {
                        throw new BackendException(
                                  "The backend store of '%s' can't " +
                                  "initialize admin user", vgraph.name());
                    }
                }
            }
            BackendStoreSystemInfo info = vgraph.backendStoreSystemInfo();
            if (!info.exists()) {
                throw new BackendException(
                          "The backend store of '%s' has not been initialized",
                          vgraph.name());
            }
            if (!info.checkVersion()) {
                throw new BackendException(
                          "The backend store version is inconsistent");
            }
        }
    }

    private void serverStarted(VortexConfig config) {
        String server = config.get(ServerOptions.SERVER_ID);
        String role = config.get(ServerOptions.SERVER_ROLE);
        E.checkArgument(StringUtils.isNotEmpty(server),
                        "The server name can't be null or empty");
        E.checkArgument(StringUtils.isNotEmpty(role),
                        "The server role can't be null or empty");
        this.server = IdGenerator.of(server);
        this.role = NodeRole.valueOf(role.toUpperCase());
        for (String graph : this.graphs()) {
            Vortex vgraph = this.graph(graph);
            assert vgraph != null;
             vgraph.serverStarted(this.server, this.role);
        }
    }

    private void addMetrics(VortexConfig config) {
        final MetricManager metric = MetricManager.INSTANCE;
        // Force to add server reporter
        ServerReporter reporter = ServerReporter.instance(metric.getRegistry());
        reporter.start(60L, TimeUnit.SECONDS);

        // Add metrics for MAX_WRITE_THREADS
        int maxWriteThreads = config.get(ServerOptions.MAX_WRITE_THREADS);
        MetricsUtil.registerGauge(RestServer.class, "max-write-threads", () -> {
            return maxWriteThreads;
        });

        // Add metrics for caches
        @SuppressWarnings({ "rawtypes", "unchecked" })
        Map<String, Cache<?, ?>> caches = (Map) CacheManager.instance()
                                                            .caches();
        registerCacheMetrics(caches);
        final AtomicInteger lastCachesSize = new AtomicInteger(caches.size());
        MetricsUtil.registerGauge(Cache.class, "instances", () -> {
            int count = caches.size();
            if (count != lastCachesSize.get()) {
                // Update if caches changed (effect in the next report period)
                registerCacheMetrics(caches);
                lastCachesSize.set(count);
            }
            return count;
        });

        // Add metrics for task
        MetricsUtil.registerGauge(TaskManager.class, "workers", () -> {
            return TaskManager.instance().workerPoolSize();
        });
        MetricsUtil.registerGauge(TaskManager.class, "pending-tasks", () -> {
            return TaskManager.instance().pendingTasks();
        });
    }

    private void listenChanges() {
        this.eventHub.listen(Events.GRAPH_CREATE, event -> {
            LOG.debug("RestServer accepts event '{}'", event.getName());
            event.checkArgs(Vortex.class);
            Vortex graph = (Vortex) event.getArgs()[0];
            this.graphs.put(graph.name(), graph);
            return null;
        });
        this.eventHub.listen(Events.GRAPH_DROP, event -> {
            LOG.debug("RestServer accepts event '{}'", event.getName());
            event.checkArgs(Vortex.class);
            Vortex graph = (Vortex) event.getArgs()[0];
            this.graphs.remove(graph.name());
            return null;
        });
    }

    private void unlistenChanges() {
        this.eventHub.unlisten(Events.GRAPH_CREATE);
        this.eventHub.unlisten(Events.GRAPH_DROP);
    }

    private void notifyAndWaitEvent(String event, Vortex graph) {
        Future<?> future = this.eventHub.notify(event, graph);
        try {
            future.get();
        } catch (Throwable e) {
            LOG.warn("Error when waiting for event execution: {}", event, e);
        }
    }

    private Vortex createGraph(VortexConfig config, String name) {
        Vortex graph = null;
        try {
            // Create graph instance
            graph = (Vortex) GraphFactory.open(config);

            // Init graph and start it
            graph.create(this.graphsDir, this.server, this.role);
        } catch (Throwable e) {
            LOG.error("Failed to create graph '{}' due to: {}",
                      name, e.getMessage(), e);
            if (graph != null) {
                this.dropGraph(graph);
            }
            throw e;
        }

        // Let gremlin server and rest server add graph to context
        this.notifyAndWaitEvent(Events.GRAPH_CREATE, graph);

        return graph;
    }

    private void dropGraph(Vortex graph) {
        // Clear data and config files
        graph.drop();

        /*
         * Will fill graph instance into VortexFactory.graphs after
         * GraphFactory.open() succeed, remove it when graph drop
         */
        VortexFactory.remove(graph);
    }

    private void checkOptions(VortexConfig config) {
        // The store cannot be the same as the existing graph
        this.checkOptionUnique(config, CoreOptions.STORE);
        /*
         * TODO: should check data path for rocksdb since can't use the same
         * data path for different graphs, but it's not easy to check here.
         */
    }

    private void checkOptionUnique(VortexConfig config,
                                   TypedOption<?, ?> option) {
        Object incomingValue = config.get(option);
        for (String graphName : this.graphs.keySet()) {
            Vortex graph = this.graph(graphName);
            Object existedValue = graph.option(option);
            E.checkArgument(!incomingValue.equals(existedValue),
                            "The value '%s' of option '%s' conflicts with " +
                            "existed graph", incomingValue, option.name());
        }
    }

    private static void registerCacheMetrics(Map<String, Cache<?, ?>> caches) {
        Set<String> names = MetricManager.INSTANCE.getRegistry().getNames();
        for (Map.Entry<String, Cache<?, ?>> entry : caches.entrySet()) {
            String key = entry.getKey();
            Cache<?, ?> cache = entry.getValue();

            String hits = String.format("%s.%s", key, "hits");
            String miss = String.format("%s.%s", key, "miss");
            String exp = String.format("%s.%s", key, "expire");
            String size = String.format("%s.%s", key, "size");
            String cap = String.format("%s.%s", key, "capacity");

            // Avoid registering multiple times
            if (names.stream().anyMatch(name -> name.endsWith(hits))) {
                continue;
            }

            MetricsUtil.registerGauge(Cache.class, hits, () -> cache.hits());
            MetricsUtil.registerGauge(Cache.class, miss, () -> cache.miss());
            MetricsUtil.registerGauge(Cache.class, exp, () -> cache.expire());
            MetricsUtil.registerGauge(Cache.class, size, () -> cache.size());
            MetricsUtil.registerGauge(Cache.class, cap, () -> cache.capacity());
        }
    }
}
