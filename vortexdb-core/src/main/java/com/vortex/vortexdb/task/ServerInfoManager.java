
package com.vortex.vortexdb.task;

import com.vortex.vortexdb.VortexException;
import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.VortexParams;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.page.PageInfo;
import com.vortex.vortexdb.backend.query.Condition;
import com.vortex.vortexdb.backend.query.ConditionQuery;
import com.vortex.vortexdb.backend.query.QueryResults;
import com.vortex.vortexdb.backend.transaction.GraphTransaction;
import com.vortex.common.event.EventListener;
import com.vortex.vortexdb.exception.ConnectionException;
import com.vortex.common.iterator.ListIterator;
import com.vortex.common.iterator.MapperIterator;
import com.vortex.vortexdb.schema.PropertyKey;
import com.vortex.vortexdb.schema.VertexLabel;
import com.vortex.vortexdb.structure.VortexVertex;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.vortexdb.type.define.VortexKeys;
import com.vortex.vortexdb.type.define.NodeRole;
import com.vortex.common.util.DateUtil;
import com.vortex.common.util.E;
import com.vortex.vortexdb.util.Events;
import com.vortex.common.util.Log;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import static com.vortex.vortexdb.backend.query.Query.NO_LIMIT;

public class ServerInfoManager {

    private static final Logger LOG = Log.logger(ServerInfoManager.class);

    public static final long MAX_SERVERS = 100000L;
    public static final long PAGE_SIZE = 10L;

    private final VortexParams graph;
    private final ExecutorService dbExecutor;
    private final EventListener eventListener;

    private Id selfServerId;
    private NodeRole selfServerRole;

    private volatile boolean onlySingleNode;
    private volatile boolean closed;

    public ServerInfoManager(VortexParams graph,
                             ExecutorService dbExecutor) {
        E.checkNotNull(graph, "graph");
        E.checkNotNull(dbExecutor, "db executor");

        this.graph = graph;
        this.dbExecutor = dbExecutor;

        this.eventListener = this.listenChanges();

        this.selfServerId = null;
        this.selfServerRole = NodeRole.MASTER;

        this.onlySingleNode = false;
        this.closed = false;
    }

    private EventListener listenChanges() {
        // Listen store event: "store.inited"
        Set<String> storeEvents = ImmutableSet.of(Events.STORE_INITED);
        EventListener eventListener = event -> {
            // Ensure server info schema create after system info initialized
            if (storeEvents.contains(event.getName())) {
                try {
                    this.initSchemaIfNeeded();
                } finally {
                    this.graph.closeTx();
                }
                return true;
            }
            return false;
        };
        this.graph.loadSystemStore().provider().listen(eventListener);
        return eventListener;
    }

    private void unlistenChanges() {
        this.graph.loadSystemStore().provider().unlisten(this.eventListener);
    }

    public boolean close() {
        this.closed = true;
        this.unlistenChanges();
        if (!this.dbExecutor.isShutdown()) {
            this.removeSelfServerInfo();
            this.call(() -> {
                try {
                    this.tx().close();
                } catch (ConnectionException ignored) {
                    // ConnectionException means no connection established
                }
                this.graph.closeTx();
                return null;
            });
        }
        return true;
    }

    public synchronized void initServerInfo(Id server, NodeRole role) {
        E.checkArgument(server != null && role != null,
                        "The server id or role can't be null");
        this.selfServerId = server;
        this.selfServerRole = role;

        VortexServerInfo existed = this.serverInfo(server);
//        E.checkArgument(existed == null || !existed.alive(),
//                        "The server with name '%s' already in cluster",
//                        server);
        if (role.master()) {
            String page = this.supportsPaging() ? PageInfo.PAGE_NONE : null;
            do {
                Iterator<VortexServerInfo> servers = this.serverInfos(PAGE_SIZE,
                                                                    page);
                while (servers.hasNext()) {
                    existed = servers.next();
//                    E.checkArgument(!existed.role().master() ||
//                                    !existed.alive(),
//                                    "Already existed master '%s' in current " +
//                                    "cluster", existed.id());
                }
                if (page != null) {
                    page = PageInfo.pageInfo(servers);
                }
            } while (page != null);
        }

        VortexServerInfo serverInfo = new VortexServerInfo(server, role);
        serverInfo.maxLoad(this.calcMaxLoad());
        this.save(serverInfo);

        LOG.info("Init server info: {}", serverInfo);
    }

    public Id selfServerId() {
        return this.selfServerId;
    }

    public NodeRole selfServerRole() {
        return this.selfServerRole;
    }

    public boolean master() {
        return this.selfServerRole != null && this.selfServerRole.master();
    }

    public boolean onlySingleNode() {
        // Only has one master node
        return this.onlySingleNode;
    }

    public void heartbeat() {
        VortexServerInfo serverInfo = this.selfServerInfo();
        if (serverInfo == null) {
            return;
        }
        serverInfo.updateTime(DateUtil.now());
        this.save(serverInfo);
    }

    public synchronized void decreaseLoad(int load) {
        assert load > 0 : load;
        VortexServerInfo serverInfo = this.selfServerInfo();
        serverInfo.increaseLoad(-load);
        this.save(serverInfo);
    }

    public int calcMaxLoad() {
        // TODO: calc max load based on CPU and Memory resources
        return 10000;
    }

    protected boolean graphReady() {
        return !this.closed && this.graph.started() && this.graph.initialized();
    }

    protected synchronized VortexServerInfo pickWorkerNode(
                                          Collection<VortexServerInfo> servers,
                                          VortexTask<?> task) {
        VortexServerInfo master = null;
        VortexServerInfo serverWithMinLoad = null;
        int minLoad = Integer.MAX_VALUE;
        boolean hasWorkerNode = false;
        long now = DateUtil.now().getTime();

        // Iterate servers to find suitable one
        for (VortexServerInfo server : servers) {
            if (!server.alive()) {
                continue;
            }

            if (server.role().master()) {
                master = server;
                continue;
            }

            hasWorkerNode = true;
            if (!server.suitableFor(task, now)) {
                continue;
            }
            if (server.load() < minLoad) {
                minLoad = server.load();
                serverWithMinLoad = server;
            }
        }

        this.onlySingleNode = !hasWorkerNode;

        // Only schedule to master if there is no workers and master is suitable
        if (!hasWorkerNode) {
            if (master != null && master.suitableFor(task, now)) {
                serverWithMinLoad = master;
            }
        }

        return serverWithMinLoad;
    }

    private void initSchemaIfNeeded() {
        VortexServerInfo.schema(this.graph).initSchemaIfNeeded();
    }

    private GraphTransaction tx() {
        assert Thread.currentThread().getName().contains("server-info-db-worker");
        return this.graph.systemTransaction();
    }

    private Id save(VortexServerInfo server) {
        return this.call(() -> {
            // Construct vertex from server info
            VortexServerInfo.Schema schema = VortexServerInfo.schema(this.graph);
            if (!schema.existVertexLabel(VortexServerInfo.P.SERVER)) {
                throw new VortexException("Schema is missing for %s '%s'",
                                        VortexServerInfo.P.SERVER, server);
            }
            VortexVertex vertex = this.tx().constructVertex(false, server.asArray());
            // Add or update server info in backend store
            vertex = this.tx().addVertex(vertex);
            return vertex.id();
        });
    }

    private int save(Collection<VortexServerInfo> servers) {
        return this.call(() -> {
            if (servers.isEmpty()) {
                return servers.size();
            }
            VortexServerInfo.Schema schema = VortexServerInfo.schema(this.graph);
            if (!schema.existVertexLabel(VortexServerInfo.P.SERVER)) {
                throw new VortexException("Schema is missing for %s",
                                        VortexServerInfo.P.SERVER);
            }
            // Save server info in batch
            GraphTransaction tx = this.tx();
            int updated = 0;
            for (VortexServerInfo server : servers) {
                if (!server.updated()) {
                    continue;
                }
                VortexVertex vertex = tx.constructVertex(false, server.asArray());
                tx.addVertex(vertex);
                updated++;
            }
            // NOTE: actually it is auto-commit, to be improved
            tx.commitOrRollback();

            return updated;
        });
    }

    private <V> V call(Callable<V> callable) {
        assert !Thread.currentThread().getName().startsWith("server-info-db-worker") : "can't call by itself";
        try {
            // Pass context for db thread
            callable = new TaskManager.ContextCallable<>(callable);
            // Ensure all db operations are executed in dbExecutor thread(s)
            return this.dbExecutor.submit(callable).get();
        } catch (Throwable e) {
            throw new VortexException("Failed to update/query server info: %s",
                                    e, e.toString());
        }
    }

    private VortexServerInfo selfServerInfo() {
        return this.serverInfo(this.selfServerId);
    }

    private VortexServerInfo serverInfo(Id server) {
        return this.call(() -> {
            Iterator<Vertex> vertices = this.tx().queryVertices(server);
            Vertex vertex = QueryResults.one(vertices);
            if (vertex == null) {
                return null;
            }
            return VortexServerInfo.fromVertex(vertex);
        });
    }

    private VortexServerInfo removeSelfServerInfo() {
        if (this.graph.initialized()) {
            return this.removeServerInfo(this.selfServerId);
        }
        return null;
    }

    private VortexServerInfo removeServerInfo(Id server) {
        if (server == null) {
            return null;
        }
        LOG.info("Remove server info: {}", server);
        return this.call(() -> {
            Iterator<Vertex> vertices = this.tx().queryVertices(server);
            Vertex vertex = QueryResults.one(vertices);
            if (vertex == null) {
                return null;
            }
            this.tx().removeVertex((VortexVertex) vertex);
            return VortexServerInfo.fromVertex(vertex);
        });
    }

    protected void updateServerInfos(Collection<VortexServerInfo> serverInfos) {
        this.save(serverInfos);
    }

    protected Collection<VortexServerInfo> allServerInfos() {
        Iterator<VortexServerInfo> infos = this.serverInfos(NO_LIMIT, null);
        try (ListIterator<VortexServerInfo> iter = new ListIterator<>(
                                                 MAX_SERVERS, infos)) {
            return iter.list();
        } catch (Exception e) {
            throw new VortexException("Failed to close server info iterator", e);
        }
    }

    protected Iterator<VortexServerInfo> serverInfos(String page) {
        return this.serverInfos(ImmutableMap.of(), PAGE_SIZE, page);
    }

    protected Iterator<VortexServerInfo> serverInfos(long limit, String page) {
        return this.serverInfos(ImmutableMap.of(), limit, page);
    }

    private Iterator<VortexServerInfo> serverInfos(Map<String, Object> conditions,
                                                   long limit, String page) {
        return this.call(() -> {
            ConditionQuery query = new ConditionQuery(VortexType.VERTEX);
            if (page != null) {
                query.page(page);
            }

            Vortex graph = this.graph.graph();
            VertexLabel vl = graph.vertexLabel(VortexServerInfo.P.SERVER);
            query.eq(VortexKeys.LABEL, vl.id());
            for (Map.Entry<String, Object> entry : conditions.entrySet()) {
                PropertyKey pk = graph.propertyKey(entry.getKey());
                query.query(Condition.eq(pk.id(), entry.getValue()));
            }
            query.showHidden(true);
            if (limit != NO_LIMIT) {
                query.limit(limit);
            }
            Iterator<Vertex> vertices = this.tx().queryVertices(query);
            Iterator<VortexServerInfo> servers =
                    new MapperIterator<>(vertices, VortexServerInfo::fromVertex);
            // Convert iterator to list to avoid across thread tx accessed
            return QueryResults.toList(servers);
        });
    }

    private boolean supportsPaging() {
        return this.graph.graph().backendStoreFeatures().supportsQueryByPage();
    }
}
