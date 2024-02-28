
package com.vortex.api.api.graph;

import com.vortex.vortexdb.Vortex;
import com.vortex.api.api.API;
import com.vortex.api.api.filter.CompressInterceptor.Compress;
import com.vortex.api.api.filter.DecompressInterceptor.Decompress;
import com.vortex.api.api.filter.StatusFilter.Status;
import com.vortex.vortexdb.backend.id.EdgeId;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.query.ConditionQuery;
import com.vortex.common.config.VortexConfig;
import com.vortex.api.config.ServerOptions;
import com.vortex.api.core.GraphManager;
import com.vortex.api.define.UpdateStrategy;
import com.vortex.vortexdb.exception.NotFoundException;
import com.vortex.vortexdb.schema.EdgeLabel;
import com.vortex.vortexdb.schema.PropertyKey;
import com.vortex.vortexdb.schema.VertexLabel;
import com.vortex.vortexdb.structure.VortexEdge;
import com.vortex.vortexdb.structure.VortexVertex;
import com.vortex.vortexdb.traversal.optimize.QueryHolder;
import com.vortex.vortexdb.traversal.optimize.TraversalUtil;
import com.vortex.vortexdb.type.define.Directions;
import com.vortex.common.util.E;
import com.vortex.common.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.function.TriFunction;
import org.slf4j.Logger;

import javax.annotation.security.RolesAllowed;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import java.util.*;

@Path("graphs/{graph}/graph/edges")
@Singleton
public class EdgeAPI extends BatchAPI {

    private static final Logger LOG = Log.logger(EdgeAPI.class);

    @POST
    @Timed(name = "single-create")
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$graph $action=edge_write"})
    public String create(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         JsonEdge jsonEdge) {
        LOG.debug("Graph [{}] create edge: {}", graph, jsonEdge);
        checkCreatingBody(jsonEdge);

        Vortex g = graph(manager, graph);

        if (jsonEdge.sourceLabel != null && jsonEdge.targetLabel != null) {
            /*
             * NOTE: If the vertex id is correct but label not match with id,
             * we allow to create it here
             */
            vertexLabel(g, jsonEdge.sourceLabel,
                        "Invalid source vertex label '%s'");
            vertexLabel(g, jsonEdge.targetLabel,
                        "Invalid target vertex label '%s'");
        }

        Vertex srcVertex = getVertex(g, jsonEdge.source, jsonEdge.sourceLabel);
        Vertex tgtVertex = getVertex(g, jsonEdge.target, jsonEdge.targetLabel);

        Edge edge = commit(g, () -> {
            return srcVertex.addEdge(jsonEdge.label, tgtVertex,
                                     jsonEdge.properties());
        });

        return manager.serializer(g).writeEdge(edge);
    }

    @POST
    @Timed(name = "batch-create")
    @Decompress
    @Path("batch")
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$graph $action=edge_write"})
    public String create(@Context VortexConfig config,
                         @Context GraphManager manager,
                         @PathParam("graph") String graph,
                         @QueryParam("check_vertex")
                         @DefaultValue("true") boolean checkVertex,
                         List<JsonEdge> jsonEdges) {
        LOG.debug("Graph [{}] create edges: {}", graph, jsonEdges);
        checkCreatingBody(jsonEdges);
        checkBatchSize(config, jsonEdges);

        Vortex g = graph(manager, graph);

        TriFunction<Vortex, Object, String, Vertex> getVertex =
                    checkVertex ? EdgeAPI::getVertex : EdgeAPI::newVertex;

        return this.commit(config, g, jsonEdges.size(), () -> {
            List<Id> ids = new ArrayList<>(jsonEdges.size());
            for (JsonEdge jsonEdge : jsonEdges) {
                /*
                 * NOTE: If the query param 'checkVertex' is false,
                 * then the label is correct and not matched id,
                 * it will be allowed currently
                 */
                Vertex srcVertex = getVertex.apply(g, jsonEdge.source,
                                                   jsonEdge.sourceLabel);
                Vertex tgtVertex = getVertex.apply(g, jsonEdge.target,
                                                   jsonEdge.targetLabel);
                Edge edge = srcVertex.addEdge(jsonEdge.label, tgtVertex,
                                              jsonEdge.properties());
                ids.add((Id) edge.id());
            }
            return manager.serializer(g).writeIds(ids);
        });
    }

    /**
     * Batch update steps are same like vertices
     */
    @PUT
    @Timed(name = "batch-update")
    @Decompress
    @Path("batch")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$graph $action=edge_write"})
    public String update(@Context VortexConfig config,
                         @Context GraphManager manager,
                         @PathParam("graph") String graph,
                         BatchEdgeRequest req) {
        BatchEdgeRequest.checkUpdate(req);
        LOG.debug("Graph [{}] update edges: {}", graph, req);
        checkUpdatingBody(req.jsonEdges);
        checkBatchSize(config, req.jsonEdges);

        Vortex g = graph(manager, graph);
        Map<Id, JsonEdge> map = new HashMap<>(req.jsonEdges.size());
        TriFunction<Vortex, Object, String, Vertex> getVertex =
                    req.checkVertex ? EdgeAPI::getVertex : EdgeAPI::newVertex;

        return this.commit(config, g, map.size(), () -> {
            // 1.Put all newEdges' properties into map (combine first)
            req.jsonEdges.forEach(newEdge -> {
                Id newEdgeId = getEdgeId(graph(manager, graph), newEdge);
                JsonEdge oldEdge = map.get(newEdgeId);
                this.updateExistElement(oldEdge, newEdge,
                                        req.updateStrategies);
                map.put(newEdgeId, newEdge);
            });

            // 2.Get all oldEdges and update with new ones
            Object[] ids = map.keySet().toArray();
            Iterator<Edge> oldEdges = g.edges(ids);
            oldEdges.forEachRemaining(oldEdge -> {
                JsonEdge newEdge = map.get(oldEdge.id());
                this.updateExistElement(g, oldEdge, newEdge,
                                        req.updateStrategies);
            });

            // 3.Add all finalEdges
            List<Edge> edges = new ArrayList<>(map.size());
            map.values().forEach(finalEdge -> {
                Vertex srcVertex = getVertex.apply(g, finalEdge.source,
                                                   finalEdge.sourceLabel);
                Vertex tgtVertex = getVertex.apply(g, finalEdge.target,
                                                   finalEdge.targetLabel);
                edges.add(srcVertex.addEdge(finalEdge.label, tgtVertex,
                                            finalEdge.properties()));
            });

            // If return ids, the ids.size() maybe different with the origins'
            return manager.serializer(g).writeEdges(edges.iterator(), false);
        });
    }

    @PUT
    @Timed(name = "single-update")
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$graph $action=edge_write"})
    public String update(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         @PathParam("id") String id,
                         @QueryParam("action") String action,
                         JsonEdge jsonEdge) {
        LOG.debug("Graph [{}] update edge: {}", graph, jsonEdge);
        checkUpdatingBody(jsonEdge);

        if (jsonEdge.id != null) {
            E.checkArgument(id.equals(jsonEdge.id),
                            "The ids are different between url and " +
                            "request body ('%s' != '%s')", id, jsonEdge.id);
        }

        // Parse action param
        boolean append = checkAndParseAction(action);

        Vortex g = graph(manager, graph);
        VortexEdge edge = (VortexEdge) g.edge(id);
        EdgeLabel edgeLabel = edge.schemaLabel();

        for (String key : jsonEdge.properties.keySet()) {
            PropertyKey pkey = g.propertyKey(key);
            E.checkArgument(edgeLabel.properties().contains(pkey.id()),
                            "Can't update property for edge '%s' because " +
                            "there is no property key '%s' in its edge label",
                            id, key);
        }

        commit(g, () -> updateProperties(edge, jsonEdge, append));

        return manager.serializer(g).writeEdge(edge);
    }

    @GET
    @Timed
    @Compress
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$graph $action=edge_read"})
    public String list(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @QueryParam("vertex_id") String vertexId,
                       @QueryParam("direction") String direction,
                       @QueryParam("label") String label,
                       @QueryParam("properties") String properties,
                       @QueryParam("keep_start_p")
                       @DefaultValue("false") boolean keepStartP,
                       @QueryParam("offset") @DefaultValue("0") long offset,
                       @QueryParam("page") String page,
                       @QueryParam("limit") @DefaultValue("100") long limit) {
        LOG.debug("Graph [{}] query edges by vertex: {}, direction: {}, " +
                  "label: {}, properties: {}, offset: {}, page: {}, limit: {}",
                  graph, vertexId, direction,
                  label, properties, offset, page, limit);

        Map<String, Object> props = parseProperties(properties);
        if (page != null) {
            E.checkArgument(offset == 0,
                            "Not support querying edges based on paging " +
                            "and offset together");
        }

        Id vertex = VertexAPI.checkAndParseVertexId(vertexId);
        Direction dir = parseDirection(direction);

        Vortex g = graph(manager, graph);

        GraphTraversal<?, Edge> traversal;
        if (vertex != null) {
            if (label != null) {
                traversal = g.traversal().V(vertex).toE(dir, label);
            } else {
                traversal = g.traversal().V(vertex).toE(dir);
            }
        } else {
            if (label != null) {
                traversal = g.traversal().E().hasLabel(label);
            } else {
                traversal = g.traversal().E();
            }
        }

        // Convert relational operator like P.gt()/P.lt()
        for (Map.Entry<String, Object> prop : props.entrySet()) {
            Object value = prop.getValue();
            if (!keepStartP && value instanceof String &&
                ((String) value).startsWith(TraversalUtil.P_CALL)) {
                prop.setValue(TraversalUtil.parsePredicate((String) value));
            }
        }

        for (Map.Entry<String, Object> entry : props.entrySet()) {
            traversal = traversal.has(entry.getKey(), entry.getValue());
        }

        if (page == null) {
            traversal = traversal.range(offset, offset + limit);
        } else {
            traversal = traversal.has(QueryHolder.SYSPROP_PAGE, page)
                                 .limit(limit);
        }

        try {
            return manager.serializer(g).writeEdges(traversal, page != null);
        } finally {
            if (g.tx().isOpen()) {
                g.tx().close();
            }
        }
    }

    @GET
    @Timed
    @Path("{id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$graph $action=edge_read"})
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @PathParam("id") String id) {
        LOG.debug("Graph [{}] get edge by id '{}'", graph, id);

        Vortex g = graph(manager, graph);
        try {
            Edge edge = g.edge(id);
            return manager.serializer(g).writeEdge(edge);
        } finally {
            if (g.tx().isOpen()) {
                g.tx().close();
            }
        }
    }

    @DELETE
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    @RolesAllowed({"admin", "$owner=$graph $action=edge_delete"})
    public void delete(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @PathParam("id") String id,
                       @QueryParam("label") String label) {
        LOG.debug("Graph [{}] remove vertex by id '{}'", graph, id);

        Vortex g = graph(manager, graph);
        commit(g, () -> {
            try {
                g.removeEdge(label, id);
            } catch (NotFoundException e) {
                throw new IllegalArgumentException(String.format(
                          "No such edge with id: '%s', %s", id, e));
            } catch (NoSuchElementException e) {
                throw new IllegalArgumentException(String.format(
                          "No such edge with id: '%s'", id));
            }
        });
    }

    private static void checkBatchSize(VortexConfig config,
                                       List<JsonEdge> edges) {
        int max = config.get(ServerOptions.MAX_EDGES_PER_BATCH);
        if (edges.size() > max) {
            throw new IllegalArgumentException(String.format(
                      "Too many edges for one time post, " +
                      "the maximum number is '%s'", max));
        }
        if (edges.size() == 0) {
            throw new IllegalArgumentException(
                      "The number of edges can't be 0");
        }
    }

    private static Vertex getVertex(Vortex graph,
                                    Object id, String label) {
        VortexVertex vertex;
        try {
            vertex = (VortexVertex) graph.vertices(id).next();
        } catch (NoSuchElementException e) {
            throw new IllegalArgumentException(String.format(
                      "Invalid vertex id '%s'", id));
        }
        if (label != null && !vertex.label().equals(label)) {
            throw new IllegalArgumentException(String.format(
                      "The label of vertex '%s' is unmatched, users expect " +
                      "label '%s', actual label stored is '%s'",
                      id, label, vertex.label()));
        }
        // Clone a new vertex to support multi-thread access
        return vertex.copy();
    }

    private static Vertex newVertex(Vortex g, Object id, String label) {
        VertexLabel vl = vertexLabel(g, label, "Invalid vertex label '%s'");
        Id idValue = VortexVertex.getIdValue(id);
        return new VortexVertex(g, idValue, vl);
    }

    private static VertexLabel vertexLabel(Vortex graph, String label,
                                           String message) {
        try {
            // NOTE: don't use SchemaManager because it will throw 404
            return graph.vertexLabel(label);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format(message, label));
        }
    }

    public static Direction parseDirection(String direction) {
        if (direction == null || direction.isEmpty()) {
            return Direction.BOTH;
        }
        try {
            return Direction.valueOf(direction);
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format(
                      "Direction value must be in [OUT, IN, BOTH], " +
                      "but got '%s'", direction));
        }
    }

    private Id getEdgeId(Vortex g, JsonEdge newEdge) {
        String sortKeys = "";
        Id labelId = g.edgeLabel(newEdge.label).id();
        List<Id> sortKeyIds = g.edgeLabel(labelId).sortKeys();
        if (!sortKeyIds.isEmpty()) {
            List<Object> sortKeyValues = new ArrayList<>(sortKeyIds.size());
            sortKeyIds.forEach(skId -> {
                PropertyKey pk = g.propertyKey(skId);
                String sortKey = pk.name();
                Object sortKeyValue = newEdge.properties.get(sortKey);
                E.checkArgument(sortKeyValue != null,
                                "The value of sort key '%s' can't be null",
                                sortKey);
                sortKeyValue = pk.validValueOrThrow(sortKeyValue);
                sortKeyValues.add(sortKeyValue);
            });
            sortKeys = ConditionQuery.concatValues(sortKeyValues);
        }
        EdgeId edgeId = new EdgeId(VortexVertex.getIdValue(newEdge.source),
                                   Directions.OUT, labelId, sortKeys,
                                   VortexVertex.getIdValue(newEdge.target));
        if (newEdge.id != null) {
            E.checkArgument(edgeId.equals(newEdge.id),
                            "The ids are different between server and " +
                            "request body ('%s' != '%s'). And note the sort " +
                            "key values should either be null or equal to " +
                            "the origin value when specified edge id",
                            edgeId, newEdge.id);
        }
        return edgeId;
    }

    protected static class BatchEdgeRequest {

        @JsonProperty("edges")
        public List<JsonEdge> jsonEdges;
        @JsonProperty("update_strategies")
        public Map<String, UpdateStrategy> updateStrategies;
        @JsonProperty("check_vertex")
        public boolean checkVertex = false;
        @JsonProperty("create_if_not_exist")
        public boolean createIfNotExist = true;

        private static void checkUpdate(BatchEdgeRequest req) {
            E.checkArgumentNotNull(req, "BatchEdgeRequest can't be null");
            E.checkArgumentNotNull(req.jsonEdges,
                                   "Parameter 'edges' can't be null");
            E.checkArgument(req.updateStrategies != null &&
                            !req.updateStrategies.isEmpty(),
                            "Parameter 'update_strategies' can't be empty");
            E.checkArgument(req.createIfNotExist == true,
                            "Parameter 'create_if_not_exist' " +
                            "dose not support false now");
        }

        @Override
        public String toString() {
            return String.format("BatchEdgeRequest{jsonEdges=%s," +
                                 "updateStrategies=%s," +
                                 "checkVertex=%s,createIfNotExist=%s}",
                                 this.jsonEdges, this.updateStrategies,
                                 this.checkVertex, this.createIfNotExist);
        }
    }

    private static class JsonEdge extends JsonElement {

        @JsonProperty("outV")
        public Object source;
        @JsonProperty("outVLabel")
        public String sourceLabel;
        @JsonProperty("inV")
        public Object target;
        @JsonProperty("inVLabel")
        public String targetLabel;

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgumentNotNull(this.label, "Expect the label of edge");
            E.checkArgumentNotNull(this.source, "Expect source vertex id");
            E.checkArgumentNotNull(this.target, "Expect target vertex id");
            if (isBatch) {
                E.checkArgumentNotNull(this.sourceLabel,
                                       "Expect source vertex label");
                E.checkArgumentNotNull(this.targetLabel,
                                       "Expect target vertex label");
            } else {
                E.checkArgument(this.sourceLabel == null &&
                                this.targetLabel == null ||
                                this.sourceLabel != null &&
                                this.targetLabel != null,
                                "The both source and target vertex label " +
                                "are either passed in, or not passed in");
            }
            this.checkUpdate();
        }

        @Override
        public void checkUpdate() {
            E.checkArgumentNotNull(this.properties,
                                   "The properties of edge can't be null");

            for (Map.Entry<String, Object> entry : this.properties.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                E.checkArgumentNotNull(value, "Not allowed to set value of " +
                                       "property '%s' to null for edge '%s'",
                                       key, this.id);
            }
        }

        @Override
        public Object[] properties() {
            return API.properties(this.properties);
        }

        @Override
        public String toString() {
            return String.format("JsonEdge{label=%s, " +
                                 "source-vertex=%s, source-vertex-label=%s, " +
                                 "target-vertex=%s, target-vertex-label=%s, " +
                                 "properties=%s}",
                                 this.label, this.source, this.sourceLabel,
                                 this.target, this.targetLabel,
                                 this.properties);
        }
    }
}
