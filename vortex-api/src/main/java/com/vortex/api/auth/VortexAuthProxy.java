
package com.vortex.api.auth;

import com.vortex.vortexdb.Vortex;
import com.vortex.api.auth.VortexAuthenticator.RolePerm;
import com.vortex.api.auth.VortexAuthenticator.User;
import com.vortex.vortexdb.auth.*;
import com.vortex.vortexdb.auth.SchemaDefine.AuthElement;
import com.vortex.vortexdb.backend.cache.Cache;
import com.vortex.vortexdb.backend.cache.CacheManager;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.id.IdGenerator;
import com.vortex.vortexdb.backend.query.Query;
import com.vortex.vortexdb.backend.store.BackendFeatures;
import com.vortex.vortexdb.backend.store.BackendStoreSystemInfo;
import com.vortex.vortexdb.backend.store.raft.RaftGroupManager;
import com.vortex.vortexdb.config.AuthOptions;
import com.vortex.common.config.VortexConfig;
import com.vortex.common.config.TypedOption;
import com.vortex.vortexdb.exception.NotSupportException;
import com.vortex.common.iterator.FilterIterator;
import com.vortex.vortexdb.rpc.RpcServiceConfig4Client;
import com.vortex.vortexdb.rpc.RpcServiceConfig4Server;
import com.vortex.vortexdb.schema.*;
import com.vortex.vortexdb.structure.VortexEdge;
import com.vortex.vortexdb.structure.VortexElement;
import com.vortex.vortexdb.structure.VortexFeatures;
import com.vortex.vortexdb.structure.VortexVertex;
import com.vortex.vortexdb.task.VortexTask;
import com.vortex.vortexdb.task.TaskManager;
import com.vortex.vortexdb.task.TaskScheduler;
import com.vortex.vortexdb.task.TaskStatus;
import com.vortex.vortexdb.traversal.optimize.VortexScriptTraversal;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.vortexdb.type.Namifiable;
import com.vortex.vortexdb.type.define.GraphMode;
import com.vortex.vortexdb.type.define.GraphReadMode;
import com.vortex.vortexdb.type.define.NodeRole;
import com.vortex.common.util.E;
import com.vortex.common.util.Log;
import com.vortex.vortexdb.util.RateLimiter;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GroovyTranslator;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode.Instruction;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal.Admin;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.slf4j.Logger;

import javax.security.sasl.AuthenticationException;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotAuthorizedException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;

public final class VortexAuthProxy implements Vortex {

    static {
        Vortex.registerTraversalStrategies(VortexAuthProxy.class);
    }

    private static final Logger LOG = Log.logger(VortexAuthProxy.class);
    private final Cache<Id, UserWithRole> usersRoleCache;
    private final Cache<Id, RateLimiter> auditLimiters;
    private final double auditLogMaxRate;

    private final Vortex graph;
    private final TaskSchedulerProxy taskScheduler;
    private final AuthManagerProxy authManager;

    public VortexAuthProxy(Vortex graph) {
        LOG.info("Wrap graph '{}' with VortexAuthProxy", graph().name());
        VortexConfig config = (VortexConfig) graph().configuration();
        long expired = config.get(AuthOptions.AUTH_CACHE_EXPIRE);
        long capacity = config.get(AuthOptions.AUTH_CACHE_CAPACITY);

        this.graph = graph();
        this.taskScheduler = new TaskSchedulerProxy(graph().taskScheduler());
        this.authManager = new AuthManagerProxy(graph.authManager());
        this.auditLimiters = this.cache("audit-log-limiter", capacity, -1L);
        this.usersRoleCache = this.cache("users-role", capacity, expired);
        this.graph().proxy(this);

        // TODO: Consider better way to get, use auth client's config now
        this.auditLogMaxRate = config.get(AuthOptions.AUTH_AUDIT_LOG_RATE);
        LOG.info("Audit log rate limit is {}/s", this.auditLogMaxRate);
    }

    @Override
    public Vortex graph() {
        this.verifyAdminPermission();
        return this.graph;
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> clazz)
                                               throws IllegalArgumentException {
        this.verifyAnyPermission();
        return this.graph().compute(clazz);
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        this.verifyAnyPermission();
        return this.graph().compute();
    }

    @Override
    public GraphTraversalSource traversal() {
        // Just return proxy
        return new GraphTraversalSourceProxy(this);
    }

    @SuppressWarnings({ "rawtypes", "deprecation" })
    @Override
    public <I extends Io> I io(final Io.Builder<I> builder) {
        this.verifyAnyPermission();
        return this.graph().io(builder);
    }

    @Override
    public SchemaManager schema() {
        SchemaManager schema = this.graph().schema();
        schema.proxy(this);
        return schema;
    }

    @Override
    public Id getNextId(VortexType type) {
        if (type == VortexType.TASK) {
            verifyPermission(VortexPermission.WRITE, ResourceType.TASK);
        } else {
            this.verifyAdminPermission();
        }
        return this.graph().getNextId(type);
    }

    @Override
    public Id addPropertyKey(PropertyKey key) {
        verifySchemaPermission(VortexPermission.WRITE, key);
        return this.graph().addPropertyKey(key);
    }

    @Override
    public Id removePropertyKey(Id key) {
        PropertyKey pkey = this.graph().propertyKey(key);
        verifySchemaPermission(VortexPermission.DELETE, pkey);
        return this.graph().removePropertyKey(key);
    }

    @Override
    public Id clearPropertyKey(PropertyKey propertyKey) {
        verifySchemaPermission(VortexPermission.DELETE, propertyKey);
        return this.graph().clearPropertyKey(propertyKey);
    }

    @Override
    public Collection<PropertyKey> propertyKeys() {
        Collection<PropertyKey> pkeys = this.graph().propertyKeys();
        return verifySchemaPermission(VortexPermission.READ, pkeys);
    }

    @Override
    public PropertyKey propertyKey(String key) {
        return verifySchemaPermission(VortexPermission.READ, () -> {
            return this.graph().propertyKey(key);
        });
    }

    @Override
    public PropertyKey propertyKey(Id key) {
        return verifySchemaPermission(VortexPermission.READ, () -> {
            return this.graph().propertyKey(key);
        });
    }

    @Override
    public boolean existsPropertyKey(String key) {
        verifyNameExistsPermission(ResourceType.PROPERTY_KEY, key);
        return this.graph().existsPropertyKey(key);
    }

    @Override
    public void addVertexLabel(VertexLabel label) {
        verifySchemaPermission(VortexPermission.WRITE, label);
        this.graph().addVertexLabel(label);
    }

    @Override
    public Id removeVertexLabel(Id id) {
        VertexLabel label = this.graph().vertexLabel(id);
        verifySchemaPermission(VortexPermission.DELETE, label);
        return this.graph().removeVertexLabel(id);
    }

    @Override
    public Collection<VertexLabel> vertexLabels() {
        Collection<VertexLabel> labels = this.graph().vertexLabels();
        return verifySchemaPermission(VortexPermission.READ, labels);
    }

    @Override
    public VertexLabel vertexLabel(String label) {
        return verifySchemaPermission(VortexPermission.READ, () -> {
            return this.graph().vertexLabel(label);
        });
    }

    @Override
    public VertexLabel vertexLabel(Id label) {
        return verifySchemaPermission(VortexPermission.READ, () -> {
            return this.graph().vertexLabel(label);
        });
    }

    @Override
    public VertexLabel vertexLabelOrNone(Id label) {
        return verifySchemaPermission(VortexPermission.READ, () -> {
            return this.graph().vertexLabelOrNone(label);
        });
    }

    @Override
    public boolean existsVertexLabel(String label) {
        verifyNameExistsPermission(ResourceType.VERTEX_LABEL, label);
        return this.graph().existsVertexLabel(label);
    }

    @Override
    public boolean existsLinkLabel(Id vertexLabel) {
        verifyNameExistsPermission(ResourceType.VERTEX_LABEL,
                                   this.vertexLabel(vertexLabel).name());
        return this.graph().existsLinkLabel(vertexLabel);
    }

    @Override
    public void addEdgeLabel(EdgeLabel label) {
        verifySchemaPermission(VortexPermission.WRITE, label);
        this.graph().addEdgeLabel(label);
    }

    @Override
    public Id removeEdgeLabel(Id id) {
        EdgeLabel label = this.graph().edgeLabel(id);
        verifySchemaPermission(VortexPermission.DELETE, label);
        return this.graph().removeEdgeLabel(id);
    }

    @Override
    public Collection<EdgeLabel> edgeLabels() {
        Collection<EdgeLabel> labels = this.graph().edgeLabels();
        return verifySchemaPermission(VortexPermission.READ, labels);
    }

    @Override
    public EdgeLabel edgeLabel(String label) {
        return verifySchemaPermission(VortexPermission.READ, () -> {
            return this.graph().edgeLabel(label);
        });
    }

    @Override
    public EdgeLabel edgeLabel(Id label) {
        return verifySchemaPermission(VortexPermission.READ, () -> {
            return this.graph().edgeLabel(label);
        });
    }

    @Override
    public EdgeLabel edgeLabelOrNone(Id label) {
        return verifySchemaPermission(VortexPermission.READ, () -> {
            return this.graph().edgeLabelOrNone(label);
        });
    }

    @Override
    public boolean existsEdgeLabel(String label) {
        verifyNameExistsPermission(ResourceType.EDGE_LABEL, label);
        return this.graph().existsEdgeLabel(label);
    }

    @Override
    public void addIndexLabel(SchemaLabel schemaLabel, IndexLabel indexLabel) {
        verifySchemaPermission(VortexPermission.WRITE, indexLabel);
        this.graph().addIndexLabel(schemaLabel, indexLabel);
    }

    @Override
    public Id removeIndexLabel(Id id) {
        IndexLabel label = this.graph().indexLabel(id);
        verifySchemaPermission(VortexPermission.DELETE, label);
        return this.graph().removeIndexLabel(id);
    }

    @Override
    public Id rebuildIndex(SchemaElement schema) {
        if (schema.type() == VortexType.INDEX_LABEL) {
            verifySchemaPermission(VortexPermission.WRITE, schema);
        } else {
            SchemaLabel label = (SchemaLabel) schema;
            for (Id il : label.indexLabels()) {
                IndexLabel indexLabel = this.graph().indexLabel(il);
                verifySchemaPermission(VortexPermission.WRITE, indexLabel);
            }
        }
        return this.graph().rebuildIndex(schema);
    }

    @Override
    public Collection<IndexLabel> indexLabels() {
        Collection<IndexLabel> labels = this.graph().indexLabels();
        return verifySchemaPermission(VortexPermission.READ, labels);
    }

    @Override
    public IndexLabel indexLabel(String label) {
        return verifySchemaPermission(VortexPermission.READ, () -> {
            return this.graph().indexLabel(label);
        });
    }

    @Override
    public IndexLabel indexLabel(Id label) {
        return verifySchemaPermission(VortexPermission.READ, () -> {
            return this.graph().indexLabel(label);
        });
    }

    @Override
    public boolean existsIndexLabel(String label) {
        verifyNameExistsPermission(ResourceType.INDEX_LABEL, label);
        return this.graph().existsIndexLabel(label);
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        return verifyElemPermission(VortexPermission.WRITE, () -> {
            return (VortexVertex) this.graph().addVertex(keyValues);
        });
    }

    @Override
    public void removeVertex(Vertex vertex) {
        verifyElemPermission(VortexPermission.DELETE, vertex);
        this.graph().removeVertex(vertex);
    }

    @Override
    public void removeVertex(String label, Object id) {
        this.removeVertex(this.vertex(id));
    }

    @Override
    public <V> void addVertexProperty(VertexProperty<V> property) {
        verifyElemPermission(VortexPermission.WRITE, property.element());
        this.graph().addVertexProperty(property);
    }

    @Override
    public <V> void removeVertexProperty(VertexProperty<V> property) {
        verifyElemPermission(VortexPermission.WRITE, property.element());
        this.graph().removeVertexProperty(property);
    }

    @Override
    public Edge addEdge(Edge edge) {
        return verifyElemPermission(VortexPermission.WRITE, () -> {
            return (VortexEdge) this.graph().addEdge(edge);
        });
    }

    @Override
    public void canAddEdge(Edge edge) {
        verifyElemPermission(VortexPermission.WRITE, () -> (VortexEdge) edge);
    }

    @Override
    public void removeEdge(Edge edge) {
        verifyElemPermission(VortexPermission.DELETE, edge);
        this.graph().removeEdge(edge);
    }

    @Override
    public void removeEdge(String label, Object id) {
        this.removeEdge(this.edge(id));
    }

    @Override
    public <V> void addEdgeProperty(Property<V> property) {
        verifyElemPermission(VortexPermission.WRITE, property.element());
        this.graph().addEdgeProperty(property);
    }

    @Override
    public <V> void removeEdgeProperty(Property<V> property) {
        verifyElemPermission(VortexPermission.WRITE, property.element());
        this.graph().removeEdgeProperty(property);
    }

    @Override
    public Iterator<Vertex> vertices(Query query) {
        return verifyElemPermission(VortexPermission.READ,
                                    this.graph().vertices(query));
    }

    @Override
    public Iterator<Vertex> vertices(Object... objects) {
        return verifyElemPermission(VortexPermission.READ,
                                    this.graph().vertices(objects));
    }

    @Override
    public Vertex vertex(Object object) {
        Vertex vertex = this.graph().vertex(object);
        verifyElemPermission(VortexPermission.READ, vertex);
        return vertex;
    }

    @Override
    public Iterator<Vertex> adjacentVertex(Object id) {
        return verifyElemPermission(VortexPermission.READ,
                                    this.graph().adjacentVertex(id));
    }

    @Override
    public Iterator<Vertex> adjacentVertices(Iterator<Edge> edges) {
        Iterator<Vertex> vertices = this.graph().adjacentVertices(edges);
        return verifyElemPermission(VortexPermission.READ, vertices);
    }

    @Override
    public boolean checkAdjacentVertexExist() {
        verifyAnyPermission();
        return this.graph().checkAdjacentVertexExist();
    }

    @Override
    public Iterator<Edge> edges(Query query) {
        return verifyElemPermission(VortexPermission.READ,
                                    this.graph().edges(query));
    }

    @Override
    public Iterator<Edge> edges(Object... objects) {
        return verifyElemPermission(VortexPermission.READ,
                                    this.graph().edges(objects));
    }

    @Override
    public Edge edge(Object id) {
        Edge edge = this.graph().edge(id);
        verifyElemPermission(VortexPermission.READ, edge);
        return edge;
    }

    @Override
    public Iterator<Edge> adjacentEdges(Id vertexId) {
        Iterator<Edge> edges = this.graph().adjacentEdges(vertexId);
        return verifyElemPermission(VortexPermission.READ, edges);
    }

    @Override
    public Number queryNumber(Query query) {
        ResourceType resType;
        if (query.resultType().isVertex()) {
            resType = ResourceType.VERTEX_AGGR;
        } else {
            assert query.resultType().isEdge();
            resType = ResourceType.EDGE_AGGR;
        }
        this.verifyPermission(VortexPermission.READ, resType);
        return this.graph().queryNumber(query);

    }

    @Override
    public Transaction tx() {
        /*
         * Can't verifyPermission() here, will be called by rollbackAll().
         */
        return this.graph().tx();
    }

    @Override
    public void close() throws Exception {
        this.verifyAdminPermission();
        this.graph().close();
    }

    @Override
    public VortexFeatures features() {
        // Can't verifyPermission() here, will be called by rollbackAll()
        //verifyStatusPermission();
        return this.graph().features();
    }

    @Override
    public Variables variables() {
        // Just return proxy
        return new VariablesProxy(this.graph().variables());
    }

    @Override
    public VortexConfig configuration() {
        throw new NotSupportException("Graph.configuration()");
    }

    @Override
    public String toString() {
        this.verifyAnyPermission();
        return this.graph().toString();
    }

    @Override
    public void proxy(Vortex graph) {
        throw new NotSupportException("Graph.proxy()");
    }

    @Override
    public boolean sameAs(Vortex graph) {
        if (graph instanceof VortexAuthProxy) {
            graph = ((VortexAuthProxy) graph).graph();
        }
        return this.graph().sameAs(graph);
    }

    @Override
    public long now() {
        // It's ok anyone call this method, so not verifyStatusPermission()
        return this.graph().now();
    }

    @Override
    public <K, V> V option(TypedOption<K, V> option) {
        this.verifyAnyPermission();
        return this.graph().option(option);
    }

    @Override
    public String name() {
        this.verifyAnyPermission();
        return this.graph().name();
    }

    @Override
    public String backend() {
        this.verifyAnyPermission();
        return this.graph().backend();
    }

    @Override
    public String backendVersion() {
        this.verifyAnyPermission();
        return this.graph().backendVersion();
    }

    @Override
    public BackendStoreSystemInfo backendStoreSystemInfo() {
        this.verifyAdminPermission();
        return this.graph().backendStoreSystemInfo();
    }

    @Override
    public BackendFeatures backendStoreFeatures() {
        this.verifyAnyPermission();
        return this.graph().backendStoreFeatures();
    }

    @Override
    public GraphMode mode() {
        this.verifyStatusPermission();
        return this.graph().mode();
    }

    @Override
    public void mode(GraphMode mode) {
        this.verifyPermission(VortexPermission.WRITE, ResourceType.STATUS);
        this.graph().mode(mode);
    }

    @Override
    public GraphReadMode readMode() {
        this.verifyStatusPermission();
        return this.graph().readMode();
    }

    @Override
    public void readMode(GraphReadMode readMode) {
        this.verifyPermission(VortexPermission.WRITE, ResourceType.STATUS);
        this.graph().readMode(readMode);
    }

    @Override
    public void waitStarted() {
        this.verifyAnyPermission();
        this.graph().waitStarted();
    }

    @Override
    public void serverStarted(Id serverId, NodeRole serverRole) {
        this.verifyAdminPermission();
        this.graph().serverStarted(serverId, serverRole);
    }

    @Override
    public boolean started() {
        this.verifyAdminPermission();
        return this.graph().started();
    }

    @Override
    public boolean closed() {
        this.verifyAdminPermission();
        return this.graph().closed();
    }

    @Override
    public <R> R metadata(VortexType type, String meta, Object... args) {
        this.verifyNamePermission(VortexPermission.EXECUTE,
                                  ResourceType.META, meta);
        return this.graph().metadata(type, meta, args);
    }

    @Override
    public TaskScheduler taskScheduler() {
        // Just return proxy
        return this.taskScheduler;
    }

    @Override
    public AuthManager authManager() {
        // Just return proxy
        return this.authManager;
    }

    @Override
    public void switchAuthManager(AuthManager authManager) {
        this.verifyAdminPermission();
        this.authManager.switchAuthManager(authManager);
    }

    @Override
    public RaftGroupManager raftGroupManager(String group) {
        this.verifyAdminPermission();
        return this.graph().raftGroupManager(group);
    }

    @Override
    public void registerRpcServices(RpcServiceConfig4Server serverConfig,
                                    RpcServiceConfig4Client clientConfig) {
        this.verifyAdminPermission();
        this.graph().registerRpcServices(serverConfig, clientConfig);
    }

    @Override
    public void initBackend() {
        this.verifyAdminPermission();
        this.graph().initBackend();
    }

    @Override
    public void clearBackend() {
        this.verifyAdminPermission();
        this.graph().clearBackend();
    }

    @Override
    public void truncateBackend() {
        this.verifyAdminPermission();
        AuthManager userManager = this.graph().authManager();
        VortexUser admin = userManager.findUser(VortexAuthenticator.USER_ADMIN);
        try {
            this.graph().truncateBackend();
        } finally {
            if (admin != null && StandardAuthManager.isLocal(userManager)) {
                // Restore admin user to continue to do any operation
                userManager.createUser(admin);
            }
        }
    }

    @Override
    public void createSnapshot() {
        this.verifyPermission(VortexPermission.WRITE, ResourceType.STATUS);
        this.graph().createSnapshot();
    }

    @Override
    public void resumeSnapshot() {
        this.verifyPermission(VortexPermission.WRITE, ResourceType.STATUS);
        this.graph().resumeSnapshot();
    }

    @Override
    public void create(String configPath, Id server, NodeRole role) {
        this.verifyPermission(VortexPermission.WRITE, ResourceType.STATUS);
        this.graph().create(configPath, server, role);
    }

    @Override
    public void drop() {
        this.verifyPermission(VortexPermission.WRITE, ResourceType.STATUS);
        this.graph().drop();
    }

    @Override
    public VortexConfig cloneConfig(String newGraph) {
        this.verifyPermission(VortexPermission.WRITE, ResourceType.STATUS);
        return this.graph().cloneConfig(newGraph);
    }

    private <V> Cache<Id, V> cache(String prefix, long capacity,
                                   long expiredTime) {
        String name = prefix + "-" + this.graph().name();
        Cache<Id, V> cache = CacheManager.instance().cache(name, capacity);
        if (expiredTime > 0L) {
            cache.expire(Duration.ofSeconds(expiredTime).toMillis());
        } else {
            cache.expire(expiredTime);
        }
        return cache;
    }

    private void verifyAdminPermission() {
        verifyPermission(VortexPermission.ANY, ResourceType.ROOT);
    }

    private void verifyStatusPermission() {
        verifyPermission(VortexPermission.READ, ResourceType.STATUS);
    }

    private void verifyAnyPermission() {
        verifyPermission(VortexPermission.READ, ResourceType.NONE);
    }

    private void verifyPermission(VortexPermission actionPerm,
                                  ResourceType resType) {
        /*
         * The owner role should match the graph name
         * NOTE: the graph names in gremlin-server.yaml/graphs and
         * graph().properties/store must be the same if enable auth.
         */
        verifyResPermission(actionPerm, true, () -> {
            String graph = this.graph().name();
            Namifiable elem = VortexResource.NameObject.ANY;
            return ResourceObject.of(graph, resType, elem);
        });
    }

    private <V extends AuthElement> V verifyUserPermission(
                                      VortexPermission actionPerm,
                                      V elementFetcher) {
        return verifyUserPermission(actionPerm, true, () -> elementFetcher);
    }

    private <V extends AuthElement> List<V> verifyUserPermission(
                                            VortexPermission actionPerm,
                                            List<V> elems) {
        List<V> results = new ArrayList<>();
        for (V elem : elems) {
            V r = verifyUserPermission(actionPerm, false, () -> elem);
            if (r != null) {
                results.add(r);
            }
        }
        return results;
    }

    private <V extends AuthElement> V verifyUserPermission(
                                      VortexPermission actionPerm,
                                      boolean throwIfNoPerm,
                                      Supplier<V> elementFetcher) {
        return verifyResPermission(actionPerm, throwIfNoPerm, () -> {
            String graph = this.graph().name();
            V elem = elementFetcher.get();
            @SuppressWarnings("unchecked")
            ResourceObject<V> r = (ResourceObject<V>) ResourceObject.of(graph,
                                                                        elem);
            return r;
        });
    }

    private void verifyElemPermission(VortexPermission actionPerm, Element elem) {
        verifyElemPermission(actionPerm, true, () -> elem);
    }

    private <V extends VortexElement> V verifyElemPermission(
                                      VortexPermission actionPerm,
                                      Supplier<V> elementFetcher) {
        return verifyElemPermission(actionPerm, true, elementFetcher);
    }

    private <V extends Element> Iterator<V> verifyElemPermission(
                                            VortexPermission actionPerm,
                                            Iterator<V> elems) {
        return new FilterIterator<>(elems, elem -> {
            V r = verifyElemPermission(actionPerm, false, () -> elem);
            return r != null;
        });
    }

    private <V extends Element> V verifyElemPermission(
                                  VortexPermission actionPerm,
                                  boolean throwIfNoPerm,
                                  Supplier<V> elementFetcher) {
        return verifyResPermission(actionPerm, throwIfNoPerm, () -> {
            String graph = this.graph().name();
            VortexElement elem = (VortexElement) elementFetcher.get();
            @SuppressWarnings("unchecked")
            ResourceObject<V> r = (ResourceObject<V>) ResourceObject.of(graph,
                                                                        elem);
            return r;
        });
    }

    private void verifyNameExistsPermission(ResourceType resType, String name) {
        verifyNamePermission(VortexPermission.READ, resType, name);
    }

    private void verifyNamePermission(VortexPermission actionPerm,
                                      ResourceType resType, String name) {
        verifyResPermission(actionPerm, true, () -> {
            String graph = this.graph().name();
            Namifiable elem = VortexResource.NameObject.of(name);
            return ResourceObject.of(graph, resType, elem);
        });
    }

    private void verifySchemaPermission(VortexPermission actionPerm,
                                        SchemaElement schema) {
        verifySchemaPermission(actionPerm, true, () -> schema);
    }

    private <V extends SchemaElement> Collection<V> verifySchemaPermission(
                                                    VortexPermission actionPerm,
                                                    Collection<V> schemas) {
        List<V> results = new ArrayList<>();
        for (V schema : schemas) {
            V r = verifySchemaPermission(actionPerm, false, () -> schema);
            if (r != null) {
                results.add(r);
            }
        }
        return results;
    }

    private <V extends SchemaElement> V verifySchemaPermission(
                                        VortexPermission actionPerm,
                                        Supplier<V> schemaFetcher) {
        return verifySchemaPermission(actionPerm, true, schemaFetcher);
    }

    private <V extends SchemaElement> V verifySchemaPermission(
                                        VortexPermission actionPerm,
                                        boolean throwIfNoPerm,
                                        Supplier<V> schemaFetcher) {
        return verifyResPermission(actionPerm, throwIfNoPerm, () -> {
            String graph = this.graph().name();
            SchemaElement elem = schemaFetcher.get();
            @SuppressWarnings("unchecked")
            ResourceObject<V> r = (ResourceObject<V>) ResourceObject.of(graph,
                                                                        elem);
            return r;
        });
    }

    private <V> V verifyResPermission(VortexPermission actionPerm,
                                      boolean throwIfNoPerm,
                                      Supplier<ResourceObject<V>> fetcher) {
        return verifyResPermission(actionPerm, throwIfNoPerm, fetcher, null);
    }

    private <V> V verifyResPermission(VortexPermission actionPerm,
                                      boolean throwIfNoPerm,
                                      Supplier<ResourceObject<V>> fetcher,
                                      Supplier<Boolean> checker) {
        // TODO: call verifyPermission() before actual action
        Context context = getContext();
        E.checkState(context != null,
                     "Missing authentication context " +
                     "when verifying resource permission");
        String username = context.user().username();
        Object role = context.user().role();
        ResourceObject<V> ro = fetcher.get();
        String action = actionPerm.string();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Verify permission {} {} for user '{}' with role {}",
                      action, ro, username, role);
        }

        V result = ro.operated();
        // Verify role permission
        if (!RolePerm.match(role, actionPerm, ro)) {
            result = null;
        }
        // Verify permission for one access another, like: granted <= user role
        else if (ro.type().isGrantOrUser()) {
            AuthElement element = (AuthElement) ro.operated();
            RolePermission grant = this.graph().authManager()
                                                 .rolePermission(element);
            if (!RolePerm.match(role, grant, ro)) {
                result = null;
            }
        }

        // Check resource detail if needed
        if (result != null && checker != null && !checker.get()) {
            result = null;
        }

        // Log user action, limit rate for each user
        Id usrId = context.user().userId();
        RateLimiter auditLimiter = this.auditLimiters.getOrFetch(usrId, id -> {
            return RateLimiter.create(this.auditLogMaxRate);
        });

        if (!(actionPerm == VortexPermission.READ && ro.type().isSchema()) &&
            auditLimiter.tryAcquire()) {
            String status = result == null ? "denied" : "allowed";
            LOG.info("User '{}' is {} to {} {}", username, status, action, ro);
        }

        // result = null means no permission, throw if needed
        if (result == null && throwIfNoPerm) {
            String error = String.format("Permission denied: %s %s",
                                         action, ro);
            throw new ForbiddenException(error);
        }
        return result;
    }

    class TaskSchedulerProxy implements TaskScheduler {

        private final TaskScheduler taskScheduler;

        public TaskSchedulerProxy(TaskScheduler origin) {
            this.taskScheduler = origin;
        }

        @Override
        public Vortex graph() {
            return this.taskScheduler.graph();
        }

        @Override
        public int pendingTasks() {
            verifyTaskPermission(VortexPermission.READ);
            return this.taskScheduler.pendingTasks();
        }

        @Override
        public <V> void restoreTasks() {
            verifyTaskPermission(VortexPermission.WRITE);
            this.taskScheduler.restoreTasks();
        }

        @Override
        public <V> Future<?> schedule(VortexTask<V> task) {
            verifyTaskPermission(VortexPermission.EXECUTE);
            task.context(getContextString());
            return this.taskScheduler.schedule(task);
        }

        @Override
        public <V> void cancel(VortexTask<V> task) {
            verifyTaskPermission(VortexPermission.WRITE, task);
            this.taskScheduler.cancel(task);
        }

        @Override
        public <V> void save(VortexTask<V> task) {
            verifyTaskPermission(VortexPermission.WRITE, task);
            this.taskScheduler.save(task);
        }

        @Override
        public <V> VortexTask<V> task(Id id) {
            return verifyTaskPermission(VortexPermission.READ,
                                        this.taskScheduler.task(id));
        }

        @Override
        public <V> Iterator<VortexTask<V>> tasks(List<Id> ids) {
            return verifyTaskPermission(VortexPermission.READ,
                                        this.taskScheduler.tasks(ids));
        }

        @Override
        public <V> Iterator<VortexTask<V>> tasks(TaskStatus status,
                                               long limit, String page) {
            Iterator<VortexTask<V>> tasks = this.taskScheduler.tasks(status,
                                                                   limit, page);
            return verifyTaskPermission(VortexPermission.READ, tasks);
        }

        @Override
        public <V> VortexTask<V> delete(Id id) {
            verifyTaskPermission(VortexPermission.DELETE,
                                 this.taskScheduler.task(id));
            return this.taskScheduler.delete(id);
        }

        @Override
        public boolean close() {
            verifyAdminPermission();
            return this.taskScheduler.close();
        }

        @Override
        public <V> VortexTask<V> waitUntilTaskCompleted(Id id, long seconds)
                                                      throws TimeoutException {
            verifyAnyPermission();
            return this.taskScheduler.waitUntilTaskCompleted(id, seconds);
        }

        @Override
        public <V> VortexTask<V> waitUntilTaskCompleted(Id id)
                                                      throws TimeoutException {
            verifyAnyPermission();
            return this.taskScheduler.waitUntilTaskCompleted(id);
        }

        @Override
        public void waitUntilAllTasksCompleted(long seconds)
                                               throws TimeoutException {
            verifyAnyPermission();
            this.taskScheduler.waitUntilAllTasksCompleted(seconds);
        }

        @Override
        public void checkRequirement(String op) {
            verifyAnyPermission();
            this.taskScheduler.checkRequirement(op);
        }

        private void verifyTaskPermission(VortexPermission actionPerm) {
            verifyPermission(actionPerm, ResourceType.TASK);
        }

        private <V> VortexTask<V> verifyTaskPermission(VortexPermission actionPerm,
                                                     VortexTask<V> task) {
            return verifyTaskPermission(actionPerm, true, task);
        }

        private <V> Iterator<VortexTask<V>> verifyTaskPermission(
                                          VortexPermission actionPerm,
                                          Iterator<VortexTask<V>> tasks) {
            return new FilterIterator<>(tasks, task -> {
                return verifyTaskPermission(actionPerm, false, task) != null;
            });
        }

        private <V> VortexTask<V> verifyTaskPermission(VortexPermission actionPerm,
                                                     boolean throwIfNoPerm,
                                                     VortexTask<V> task) {
            Object r = verifyResPermission(actionPerm, throwIfNoPerm, () -> {
                String graph = VortexAuthProxy.this.graph().name();
                String name = task.id().toString();
                Namifiable elem = VortexResource.NameObject.of(name);
                return ResourceObject.of(graph, ResourceType.TASK, elem);
            }, () -> {
                return hasTaskPermission(task);
            });
            return r == null ? null : task;
        }

        private boolean hasTaskPermission(VortexTask<?> task) {
            Context context = getContext();
            if (context == null) {
                return false;
            }
            User currentUser = context.user();

            User taskUser = User.fromJson(task.context());
            if (taskUser == null) {
                return User.ADMIN.equals(currentUser);
            }

            return Objects.equals(currentUser.getName(), taskUser.getName()) ||
                   RolePerm.match(currentUser.role(), taskUser.role(), null);
        }
    }

    class AuthManagerProxy implements AuthManager {

        private AuthManager authManager;

        public AuthManagerProxy(AuthManager origin) {
            this.authManager = origin;
        }

        private AuthElement updateCreator(AuthElement elem) {
            String username = currentUsername();
            if (username != null && elem.creator() == null) {
                elem.creator(username);
            }
            return elem;
        }

        private String currentUsername() {
            Context context = getContext();
            if (context != null) {
                return context.user().username();
            }
            return null;
        }

        @Override
        public boolean close() {
            verifyAdminPermission();
            return this.authManager.close();
        }

        @Override
        public Id createUser(VortexUser user) {
            E.checkArgument(!VortexAuthenticator.USER_ADMIN.equals(user.name()),
                            "Invalid user name '%s'", user.name());
            this.updateCreator(user);
            verifyUserPermission(VortexPermission.WRITE, user);
            return this.authManager.createUser(user);
        }

        @Override
        public Id updateUser(VortexUser updatedUser) {
            String username = currentUsername();
            VortexUser user = this.authManager.getUser(updatedUser.id());
            if (!user.name().equals(username)) {
                this.updateCreator(updatedUser);
                verifyUserPermission(VortexPermission.WRITE, user);
            }
            this.invalidRoleCache();
            return this.authManager.updateUser(updatedUser);
        }

        @Override
        public VortexUser deleteUser(Id id) {
            VortexUser user = this.authManager.getUser(id);
            E.checkArgument(!VortexAuthenticator.USER_ADMIN.equals(user.name()),
                            "Can't delete user '%s'", user.name());
            verifyUserPermission(VortexPermission.DELETE, user);
            VortexAuthProxy.this.auditLimiters.invalidate(user.id());
            this.invalidRoleCache();
            return this.authManager.deleteUser(id);
        }

        @Override
        public VortexUser findUser(String name) {
            VortexUser user = this.authManager.findUser(name);
            String username = currentUsername();
            if (!user.name().equals(username)) {
                verifyUserPermission(VortexPermission.READ, user);
            }
            return user;
        }

        @Override
        public VortexUser getUser(Id id) {
            VortexUser user = this.authManager.getUser(id);
            String username = currentUsername();
            if (!user.name().equals(username)) {
                verifyUserPermission(VortexPermission.READ, user);
            }
            return user;
        }

        @Override
        public List<VortexUser> listUsers(List<Id> ids) {
            return verifyUserPermission(VortexPermission.READ,
                                        this.authManager.listUsers(ids));
        }

        @Override
        public List<VortexUser> listAllUsers(long limit) {
            return verifyUserPermission(VortexPermission.READ,
                                        this.authManager.listAllUsers(limit));
        }

        @Override
        public Id createGroup(VortexGroup group) {
            this.updateCreator(group);
            verifyUserPermission(VortexPermission.WRITE, group);
            this.invalidRoleCache();
            return this.authManager.createGroup(group);
        }

        @Override
        public Id updateGroup(VortexGroup group) {
            this.updateCreator(group);
            verifyUserPermission(VortexPermission.WRITE, group);
            this.invalidRoleCache();
            return this.authManager.updateGroup(group);
        }

        @Override
        public VortexGroup deleteGroup(Id id) {
            verifyUserPermission(VortexPermission.DELETE,
                                 this.authManager.getGroup(id));
            this.invalidRoleCache();
            return this.authManager.deleteGroup(id);
        }

        @Override
        public VortexGroup getGroup(Id id) {
            return verifyUserPermission(VortexPermission.READ,
                                        this.authManager.getGroup(id));
        }

        @Override
        public List<VortexGroup> listGroups(List<Id> ids) {
            return verifyUserPermission(VortexPermission.READ,
                                        this.authManager.listGroups(ids));
        }

        @Override
        public List<VortexGroup> listAllGroups(long limit) {
            return verifyUserPermission(VortexPermission.READ,
                                        this.authManager.listAllGroups(limit));
        }

        @Override
        public Id createTarget(VortexTarget target) {
            this.updateCreator(target);
            verifyUserPermission(VortexPermission.WRITE, target);
            this.invalidRoleCache();
            return this.authManager.createTarget(target);
        }

        @Override
        public Id updateTarget(VortexTarget target) {
            this.updateCreator(target);
            verifyUserPermission(VortexPermission.WRITE, target);
            this.invalidRoleCache();
            return this.authManager.updateTarget(target);
        }

        @Override
        public VortexTarget deleteTarget(Id id) {
            verifyUserPermission(VortexPermission.DELETE,
                                 this.authManager.getTarget(id));
            this.invalidRoleCache();
            return this.authManager.deleteTarget(id);
        }

        @Override
        public VortexTarget getTarget(Id id) {
            return verifyUserPermission(VortexPermission.READ,
                                        this.authManager.getTarget(id));
        }

        @Override
        public List<VortexTarget> listTargets(List<Id> ids) {
            return verifyUserPermission(VortexPermission.READ,
                                        this.authManager.listTargets(ids));
        }

        @Override
        public List<VortexTarget> listAllTargets(long limit) {
            return verifyUserPermission(VortexPermission.READ,
                                        this.authManager.listAllTargets(limit));
        }

        @Override
        public Id createBelong(VortexBelong belong) {
            this.updateCreator(belong);
            verifyUserPermission(VortexPermission.WRITE, belong);
            this.invalidRoleCache();
            return this.authManager.createBelong(belong);
        }

        @Override
        public Id updateBelong(VortexBelong belong) {
            this.updateCreator(belong);
            verifyUserPermission(VortexPermission.WRITE, belong);
            this.invalidRoleCache();
            return this.authManager.updateBelong(belong);
        }

        @Override
        public VortexBelong deleteBelong(Id id) {
            verifyUserPermission(VortexPermission.DELETE,
                                 this.authManager.getBelong(id));
            this.invalidRoleCache();
            return this.authManager.deleteBelong(id);
        }

        @Override
        public VortexBelong getBelong(Id id) {
            return verifyUserPermission(VortexPermission.READ,
                                        this.authManager.getBelong(id));
        }

        @Override
        public List<VortexBelong> listBelong(List<Id> ids) {
            return verifyUserPermission(VortexPermission.READ,
                                        this.authManager.listBelong(ids));
        }

        @Override
        public List<VortexBelong> listAllBelong(long limit) {
            return verifyUserPermission(VortexPermission.READ,
                                        this.authManager.listAllBelong(limit));
        }

        @Override
        public List<VortexBelong> listBelongByUser(Id user, long limit) {
            List<VortexBelong> r = this.authManager.listBelongByUser(user, limit);
            return verifyUserPermission(VortexPermission.READ, r);
        }

        @Override
        public List<VortexBelong> listBelongByGroup(Id group, long limit) {
            List<VortexBelong> r = this.authManager.listBelongByGroup(group,
                                                                    limit);
            return verifyUserPermission(VortexPermission.READ, r);
        }

        @Override
        public Id createAccess(VortexAccess access) {
            this.updateCreator(access);
            verifyUserPermission(VortexPermission.WRITE, access);
            this.invalidRoleCache();
            return this.authManager.createAccess(access);
        }

        @Override
        public Id updateAccess(VortexAccess access) {
            this.updateCreator(access);
            verifyUserPermission(VortexPermission.WRITE, access);
            this.invalidRoleCache();
            return this.authManager.updateAccess(access);
        }

        @Override
        public VortexAccess deleteAccess(Id id) {
            verifyUserPermission(VortexPermission.DELETE,
                                 this.authManager.getAccess(id));
            this.invalidRoleCache();
            return this.authManager.deleteAccess(id);
        }

        @Override
        public VortexAccess getAccess(Id id) {
            return verifyUserPermission(VortexPermission.READ,
                                        this.authManager.getAccess(id));
        }

        @Override
        public List<VortexAccess> listAccess(List<Id> ids) {
            return verifyUserPermission(VortexPermission.READ,
                                        this.authManager.listAccess(ids));
        }

        @Override
        public List<VortexAccess> listAllAccess(long limit) {
            return verifyUserPermission(VortexPermission.READ,
                                        this.authManager.listAllAccess(limit));
        }

        @Override
        public List<VortexAccess> listAccessByGroup(Id group, long limit) {
            List<VortexAccess> r = this.authManager.listAccessByGroup(group,
                                                                    limit);
            return verifyUserPermission(VortexPermission.READ, r);
        }

        @Override
        public List<VortexAccess> listAccessByTarget(Id target, long limit) {
            List<VortexAccess> r = this.authManager.listAccessByTarget(target,
                                                                     limit);
            return verifyUserPermission(VortexPermission.READ, r);
        }

        @Override
        public Id createProject(VortexProject project) {
            this.updateCreator(project);
            verifyUserPermission(VortexPermission.WRITE, project);
            return this.authManager.createProject(project);
        }

        @Override
        public VortexProject deleteProject(Id id) {
            verifyUserPermission(VortexPermission.DELETE,
                                 this.authManager.getProject(id));
            return this.authManager.deleteProject(id);
        }

        @Override
        public Id updateProject(VortexProject project) {
            this.updateCreator(project);
            verifyUserPermission(VortexPermission.WRITE, project);
            return this.authManager.updateProject(project);
        }

        @Override
        public Id projectAddGraphs(Id id, Set<String> graphs) {
            verifyUserPermission(VortexPermission.WRITE,
                                 this.authManager.getProject(id));
            return this.authManager.projectAddGraphs(id, graphs);
        }

        @Override
        public Id projectRemoveGraphs(Id id, Set<String> graphs) {
            verifyUserPermission(VortexPermission.WRITE,
                                 this.authManager.getProject(id));
            return this.authManager.projectRemoveGraphs(id, graphs);
        }

        @Override
        public VortexProject getProject(Id id) {
            VortexProject project = this.authManager.getProject(id);
            verifyUserPermission(VortexPermission.READ, project);
            return project;
        }

        @Override
        public List<VortexProject> listAllProject(long limit) {
            List<VortexProject> projects = this.authManager.listAllProject(limit);
            return verifyUserPermission(VortexPermission.READ, projects);
        }

        @Override
        public VortexUser matchUser(String name, String password) {
            // Unneeded to verify permission
            return this.authManager.matchUser(name, password);
        }

        @Override
        public RolePermission rolePermission(AuthElement element) {
            String username = currentUsername();
            if (!(element instanceof VortexUser) ||
                !((VortexUser) element).name().equals(username)) {
                verifyUserPermission(VortexPermission.READ, element);
            }
            return this.authManager.rolePermission(element);
        }

        @Override
        public UserWithRole validateUser(String username, String password) {
            // Can't verifyPermission() here, validate first with tmp permission
            Context context = setContext(Context.admin());

            try {
                Id userKey = IdGenerator.of(username + password);
                return VortexAuthProxy.this.usersRoleCache.getOrFetch(userKey, id -> {
                    return this.authManager.validateUser(username, password);
                });
            } catch (Exception e) {
                LOG.error("Failed to validate user {} with error: ",
                          username, e);
                throw e;
            } finally {
                setContext(context);
            }
        }

        @Override
        public UserWithRole validateUser(String token) {
            // Can't verifyPermission() here, validate first with tmp permission
            Context context = setContext(Context.admin());

            try {
                Id userKey = IdGenerator.of(token);
                return VortexAuthProxy.this.usersRoleCache.getOrFetch(userKey, id -> {
                    return this.authManager.validateUser(token);
                });
            } catch (Exception e) {
                LOG.error("Failed to validate token {} with error: ", token, e);
                throw e;
            } finally {
                setContext(context);
            }
        }

        @Override
        public String loginUser(String username, String password) {
            try {
                return this.authManager.loginUser(username, password);
            } catch (AuthenticationException e) {
                throw new NotAuthorizedException(e.getMessage(), e);
            }
        }

        @Override
        public void logoutUser(String token) {
            this.authManager.logoutUser(token);
        }

        private void switchAuthManager(AuthManager authManager) {
            this.authManager = authManager;
            VortexAuthProxy.this.graph().switchAuthManager(authManager);
        }

        private void invalidRoleCache() {
            VortexAuthProxy.this.usersRoleCache.clear();
        }
    }

    class VariablesProxy implements Variables {

        private final Variables variables;

        public VariablesProxy(Variables variables) {
            this.variables = variables;
        }

        @Override
        public <R> Optional<R> get(String key) {
            verifyPermission(VortexPermission.READ, ResourceType.VAR);
            return this.variables.get(key);
        }

        @Override
        public Set<String> keys() {
            verifyPermission(VortexPermission.READ, ResourceType.VAR);
            return this.variables.keys();
        }

        @Override
        public void set(String key, Object value) {
            verifyPermission(VortexPermission.WRITE, ResourceType.VAR);
            this.variables.set(key, value);
        }

        @Override
        public void remove(String key) {
            verifyPermission(VortexPermission.DELETE, ResourceType.VAR);
            this.variables.remove(key);
        }
    }

    class GraphTraversalSourceProxy extends GraphTraversalSource {

        public GraphTraversalSourceProxy(Graph graph) {
            super(graph);
        }

        public GraphTraversalSourceProxy(Graph graph,
                                         TraversalStrategies strategies) {
            super(graph, strategies);
        }

        @Override
        public TraversalStrategies getStrategies() {
            // getStrategies()/getGraph() is called by super.clone()
            return new TraversalStrategiesProxy(super.getStrategies());
        }
    }

    class TraversalStrategiesProxy implements TraversalStrategies {

        private static final String REST_WORKER = "grizzly-http-server";
        private static final long serialVersionUID = -5424364720492307019L;
        private final TraversalStrategies strategies;

        public TraversalStrategiesProxy(TraversalStrategies strategies) {
            this.strategies = strategies;
        }

        @Override
        public List<TraversalStrategy<?>> toList() {
            return this.strategies.toList();
        }

        @Override
        public void applyStrategies(Admin<?, ?> traversal) {
            String script;
            if (traversal instanceof VortexScriptTraversal) {
                script = ((VortexScriptTraversal<?, ?>) traversal).script();
            } else {
                GroovyTranslator translator = GroovyTranslator.of("g");
                script = translator.translate(traversal.getBytecode());
            }

            /*
             * Verify gremlin-execute permission for user gremlin(in gremlin-
             * server-exec worker) and gremlin job(in task worker).
             * But don't check permission in rest worker, because the following
             * places need to call traversal():
             *  1.vertices/edges rest api
             *  2.oltp rest api (like crosspointpath/neighborrank)
             *  3.olap rest api (like centrality/lpa/louvain/subgraph)
             */
            String caller = Thread.currentThread().getName();
            if (!caller.contains(REST_WORKER)) {
                verifyNamePermission(VortexPermission.EXECUTE,
                                     ResourceType.GREMLIN, script);
            }

            this.strategies.applyStrategies(traversal);
        }

        @Override
        public TraversalStrategies addStrategies(TraversalStrategy<?>...
                                                 strategies) {
            return this.strategies.addStrategies(strategies);
        }

        @SuppressWarnings({ "rawtypes", "unchecked" })
        @Override
        public TraversalStrategies removeStrategies(
               Class<? extends TraversalStrategy>... strategyClasses) {
            return this.strategies.removeStrategies(strategyClasses);
        }

        @Override
        public TraversalStrategies clone() {
            return this.strategies.clone();
        }

        @SuppressWarnings("unused")
        private String translate(Bytecode bytecode) {
            // GroovyTranslator.of("g").translate(bytecode);
            List<Instruction> steps = bytecode.getStepInstructions();
            StringBuilder sb = new StringBuilder();
            sb.append("g");
            int stepsPrint = Math.min(10, steps.size());
            for (int i = 0; i < stepsPrint; i++) {
                Instruction step = steps.get(i);
                sb.append('.').append(step);
            }
            if (stepsPrint < steps.size()) {
                sb.append("..");
            }
            return sb.toString();
        }
    }

    private static final ThreadLocal<Context> contexts =
                                              new InheritableThreadLocal<>();

    protected static final Context setContext(Context context) {
        Context old = contexts.get();
        contexts.set(context);
        return old;
    }

    protected static final void resetContext() {
        contexts.remove();
    }

    protected static final Context getContext() {
        // Return task context first
        String taskContext = TaskManager.getContext();
        User user = User.fromJson(taskContext);
        if (user != null) {
            return new Context(user);
        }

        return contexts.get();
    }

    protected static final String getContextString() {
        Context context = getContext();
        if (context == null) {
            return null;
        }
        return context.user().toJson();
    }

    protected static final void logUser(User user, String path) {
        LOG.info("User '{}' login from client [{}] with path '{}'",
                 user.username(), user.client(), path);
    }

    static class Context {

        private static final Context ADMIN = new Context(User.ADMIN);

        private final User user;

        public Context(User user) {
            E.checkNotNull(user, "user");
            this.user = user;
        }

        public User user() {
            return this.user;
        }

        public static Context admin() {
            return ADMIN;
        }
    }

    static class ContextTask implements Runnable {

        private final Runnable runner;
        private final Context context;

        public ContextTask(Runnable runner) {
            this.context = getContext();
            this.runner = runner;
        }

        @Override
        public void run() {
            setContext(this.context);
            try {
                this.runner.run();
            } finally {
                resetContext();
            }
        }
    }

    public static class ContextThreadPoolExecutor extends ThreadPoolExecutor {

        public ContextThreadPoolExecutor(int corePoolSize, int maxPoolSize,
                                         ThreadFactory threadFactory) {
            super(corePoolSize, maxPoolSize, 0L, TimeUnit.MILLISECONDS,
                  new LinkedBlockingQueue<Runnable>(), threadFactory);
        }

        @Override
        public void execute(Runnable command) {
            super.execute(new ContextTask(command));
        }
    }
}
