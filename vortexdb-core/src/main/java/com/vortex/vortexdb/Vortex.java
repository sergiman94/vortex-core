package com.vortex.vortexdb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import com.vortex.vortexdb.auth.AuthManager;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.query.Query;
import com.vortex.vortexdb.backend.store.BackendFeatures;
import com.vortex.vortexdb.backend.store.BackendStoreSystemInfo;
import com.vortex.vortexdb.backend.store.raft.RaftGroupManager;
import com.vortex.common.config.VortexConfig;
import com.vortex.common.config.TypedOption;
import com.vortex.vortexdb.rpc.RpcServiceConfig4Client;
import com.vortex.vortexdb.rpc.RpcServiceConfig4Server;
import com.vortex.vortexdb.schema.EdgeLabel;
import com.vortex.vortexdb.schema.IndexLabel;
import com.vortex.vortexdb.schema.PropertyKey;
import com.vortex.vortexdb.schema.SchemaElement;
import com.vortex.vortexdb.schema.SchemaLabel;
import com.vortex.vortexdb.schema.SchemaManager;
import com.vortex.vortexdb.schema.VertexLabel;
import com.vortex.vortexdb.structure.VortexFeatures;
import com.vortex.vortexdb.task.TaskScheduler;
import com.vortex.vortexdb.traversal.optimize.VortexCountStepStrategy;
import com.vortex.vortexdb.traversal.optimize.VortexStepStrategy;
import com.vortex.vortexdb.traversal.optimize.VortexVertexStepStrategy;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.vortexdb.type.define.GraphMode;
import com.vortex.vortexdb.type.define.GraphReadMode;
import com.vortex.vortexdb.type.define.NodeRole;

public interface Vortex extends Graph {

    public Vortex graph();

    public SchemaManager schema();

    public Id getNextId(VortexType type);

    public Id addPropertyKey(PropertyKey key);
    public Id removePropertyKey(Id key);
    public Id clearPropertyKey(PropertyKey propertyKey);
    public Collection<PropertyKey> propertyKeys();
    public PropertyKey propertyKey(String key);
    public PropertyKey propertyKey(Id key);
    public boolean existsPropertyKey(String key);

    public void addVertexLabel(VertexLabel vertexLabel);
    public Id removeVertexLabel(Id label);
    public Collection<VertexLabel> vertexLabels();
    public VertexLabel vertexLabel(String label);
    public VertexLabel vertexLabel(Id label);
    public VertexLabel vertexLabelOrNone(Id id);
    public boolean existsVertexLabel(String label);
    public boolean existsLinkLabel(Id vertexLabel);

    public void addEdgeLabel(EdgeLabel edgeLabel);
    public Id removeEdgeLabel(Id label);
    public Collection<EdgeLabel> edgeLabels();
    public EdgeLabel edgeLabel(String label);
    public EdgeLabel edgeLabel(Id label);
    public EdgeLabel edgeLabelOrNone(Id label);
    public boolean existsEdgeLabel(String label);

    public void addIndexLabel(SchemaLabel schemaLabel, IndexLabel indexLabel);
    public Id removeIndexLabel(Id label);
    public Id rebuildIndex(SchemaElement schema);
    public Collection<IndexLabel> indexLabels();
    public IndexLabel indexLabel(String label);
    public IndexLabel indexLabel(Id id);
    public boolean existsIndexLabel(String label);

    @Override
    public Vertex addVertex(Object... keyValues);
    public void removeVertex(Vertex vertex);
    public void removeVertex(String label, Object id);
    public <V> void addVertexProperty(VertexProperty<V> property);
    public <V> void removeVertexProperty(VertexProperty<V> property);

    public Edge addEdge(Edge edge);
    public void canAddEdge(Edge edge);
    public void removeEdge(Edge edge);
    public void removeEdge(String label, Object id);
    public <V> void addEdgeProperty(Property<V> property);
    public <V> void removeEdgeProperty(Property<V> property);

    public Vertex vertex(Object object);
    @Override
    public Iterator<Vertex> vertices(Object... objects);
    public Iterator<Vertex> vertices(Query query);
    public Iterator<Vertex> adjacentVertex(Object id);
    public boolean checkAdjacentVertexExist();

    public Edge edge(Object object);
    @Override
    public Iterator<Edge> edges(Object... objects);
    public Iterator<Edge> edges(Query query);
    public Iterator<Vertex> adjacentVertices(Iterator<Edge> edges) ;
    public Iterator<Edge> adjacentEdges(Id vertexId);

    public Number queryNumber(Query query);

    public String name();
    public String backend();
    public String backendVersion();
    public BackendStoreSystemInfo backendStoreSystemInfo();
    public BackendFeatures backendStoreFeatures();

    public GraphMode mode();
    public void mode(GraphMode mode);

    public GraphReadMode readMode();
    public void readMode(GraphReadMode readMode);

    public void waitStarted();
    public void serverStarted(Id serverId, NodeRole serverRole);
    public boolean started();
    public boolean closed();

    public <T> T metadata(VortexType type, String meta, Object... args);

    public void initBackend();
    public void clearBackend();
    public void truncateBackend();

    public void createSnapshot();
    public void resumeSnapshot();

    public void create(String configPath, Id server, NodeRole role);
    public void drop();

    public VortexConfig cloneConfig(String newGraph);

    @Override
    public VortexFeatures features();

    public AuthManager authManager();
    public void switchAuthManager(AuthManager authManager);
    public TaskScheduler taskScheduler();
    public RaftGroupManager raftGroupManager(String group);

    public void proxy(Vortex graph);

    public boolean sameAs(Vortex graph);

    public long now();

    public <K, V> V option(TypedOption<K, V> option);

    public void registerRpcServices(RpcServiceConfig4Server serverConfig,
                                    RpcServiceConfig4Client clientConfig);

    public default List<String> mapPkId2Name(Collection<Id> ids) {
        List<String> names = new ArrayList<>(ids.size());
        for (Id id : ids) {
            SchemaElement schema = this.propertyKey(id);
            names.add(schema.name());
        }
        return names;
    }

    public default List<String> mapVlId2Name(Collection<Id> ids) {
        List<String> names = new ArrayList<>(ids.size());
        for (Id id : ids) {
            SchemaElement schema = this.vertexLabel(id);
            names.add(schema.name());
        }
        return names;
    }

    public default List<String> mapElId2Name(Collection<Id> ids) {
        List<String> names = new ArrayList<>(ids.size());
        for (Id id : ids) {
            SchemaElement schema = this.edgeLabel(id);
            names.add(schema.name());
        }
        return names;
    }

    public default List<String> mapIlId2Name(Collection<Id> ids) {
        List<String> names = new ArrayList<>(ids.size());
        for (Id id : ids) {
            SchemaElement schema = this.indexLabel(id);
            names.add(schema.name());
        }
        return names;
    }

    public default List<Id> mapPkName2Id(Collection<String> pkeys) {
        List<Id> ids = new ArrayList<>(pkeys.size());
        for (String pkey : pkeys) {
            PropertyKey propertyKey = this.propertyKey(pkey);
            ids.add(propertyKey.id());
        }
        return ids;
    }

    public default Id[] mapElName2Id(String[] edgeLabels) {
        Id[] ids = new Id[edgeLabels.length];
        for (int i = 0; i < edgeLabels.length; i++) {
            EdgeLabel edgeLabel = this.edgeLabel(edgeLabels[i]);
            ids[i] = edgeLabel.id();
        }
        return ids;
    }

    public default Id[] mapVlName2Id(String[] vertexLabels) {
        Id[] ids = new Id[vertexLabels.length];
        for (int i = 0; i < vertexLabels.length; i++) {
            VertexLabel vertexLabel = this.vertexLabel(vertexLabels[i]);
            ids[i] = vertexLabel.id();
        }
        return ids;
    }

    public static void registerTraversalStrategies(Class<?> clazz) {
        TraversalStrategies strategies = null;
        strategies = TraversalStrategies.GlobalCache
                .getStrategies(Graph.class)
                .clone();
        strategies.addStrategies(VortexVertexStepStrategy.instance(),
                VortexStepStrategy.instance(),
                VortexCountStepStrategy.instance());
        TraversalStrategies.GlobalCache.registerStrategies(clazz, strategies);
    }
}
