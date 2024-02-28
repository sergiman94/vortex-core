
package com.vortex.api.api.graph;

import com.vortex.vortexdb.Vortex;
import com.vortex.api.api.API;
import com.vortex.api.api.filter.CompressInterceptor.Compress;
import com.vortex.api.api.filter.DecompressInterceptor.Decompress;
import com.vortex.api.api.filter.StatusFilter.Status;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.id.SplicingIdGenerator;
import com.vortex.vortexdb.backend.query.ConditionQuery;
import com.vortex.common.config.VortexConfig;
import com.vortex.api.config.ServerOptions;
import com.vortex.api.core.GraphManager;
import com.vortex.api.define.UpdateStrategy;
import com.vortex.vortexdb.exception.NotFoundException;
import com.vortex.vortexdb.schema.PropertyKey;
import com.vortex.vortexdb.schema.VertexLabel;
import com.vortex.vortexdb.structure.VortexVertex;
import com.vortex.vortexdb.traversal.optimize.QueryHolder;
import com.vortex.vortexdb.traversal.optimize.Text;
import com.vortex.vortexdb.traversal.optimize.TraversalUtil;
import com.vortex.vortexdb.type.define.IdStrategy;
import com.vortex.common.util.E;
import com.vortex.vortexdb.util.JsonUtil;
import com.vortex.common.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import javax.annotation.security.RolesAllowed;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import java.util.*;

@Path("graphs/{graph}/graph/vertices")
@Singleton
public class VertexAPI extends BatchAPI {

    private static final Logger LOG = Log.logger(VertexAPI.class);

    @POST
    @Timed(name = "single-create")
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$graph $action=vertex_write"})
    public String create(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         JsonVertex jsonVertex) {
        LOG.debug("Graph [{}] create vertex: {}", graph, jsonVertex);
        checkCreatingBody(jsonVertex);

        Vortex g = graph(manager, graph);
        Vertex vertex = commit(g, () -> g.addVertex(jsonVertex.properties()));

        return manager.serializer(g).writeVertex(vertex);
    }

    @POST
    @Timed(name = "batch-create")
    @Decompress
    @Path("batch")
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$graph $action=vertex_write"})
    public String create(@Context VortexConfig config,
                         @Context GraphManager manager,
                         @PathParam("graph") String graph,
                         List<JsonVertex> jsonVertices) {
        LOG.debug("Graph [{}] create vertices: {}", graph, jsonVertices);
        checkCreatingBody(jsonVertices);
        checkBatchSize(config, jsonVertices);

        Vortex g = graph(manager, graph);

        return this.commit(config, g, jsonVertices.size(), () -> {
            List<Id> ids = new ArrayList<>(jsonVertices.size());
            for (JsonVertex vertex : jsonVertices) {
                ids.add((Id) g.addVertex(vertex.properties()).id());
            }
            return manager.serializer(g).writeIds(ids);
        });
    }

    /**
     * Batch update steps like:
     * 1. Get all newVertices' ID & combine first
     * 2. Get all oldVertices & update
     * 3. Add the final vertex together
     */
    @PUT
    @Timed(name = "batch-update")
    @Decompress
    @Path("batch")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$graph $action=vertex_write"})
    public String update(@Context VortexConfig config,
                         @Context GraphManager manager,
                         @PathParam("graph") String graph,
                         BatchVertexRequest req) {
        BatchVertexRequest.checkUpdate(req);
        LOG.debug("Graph [{}] update vertices: {}", graph, req);
        checkUpdatingBody(req.jsonVertices);
        checkBatchSize(config, req.jsonVertices);

        Vortex g = graph(manager, graph);
        Map<Id, JsonVertex> map = new HashMap<>(req.jsonVertices.size());

        return this.commit(config, g, map.size(), () -> {
            /*
             * 1.Put all newVertices' properties into map (combine first)
             * - Consider primary-key & user-define ID mode first
             */
            req.jsonVertices.forEach(newVertex -> {
                Id newVertexId = getVertexId(g, newVertex);
                JsonVertex oldVertex = map.get(newVertexId);
                this.updateExistElement(oldVertex, newVertex,
                                        req.updateStrategies);
                map.put(newVertexId, newVertex);
            });

            // 2.Get all oldVertices and update with new vertices
            Object[] ids = map.keySet().toArray();
            Iterator<Vertex> oldVertices = g.vertices(ids);
            oldVertices.forEachRemaining(oldVertex -> {
                JsonVertex newVertex = map.get(oldVertex.id());
                this.updateExistElement(g, oldVertex, newVertex,
                                        req.updateStrategies);
            });

            // 3.Add finalVertices and return them
            List<Vertex> vertices = new ArrayList<>(map.size());
            map.values().forEach(finalVertex -> {
                vertices.add(g.addVertex(finalVertex.properties()));
            });

            // If return ids, the ids.size() maybe different with the origins'
            return manager.serializer(g)
                          .writeVertices(vertices.iterator(), false);
        });
    }

    @PUT
    @Timed(name = "single-update")
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$graph $action=vertex_write"})
    public String update(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         @PathParam("id") String idValue,
                         @QueryParam("action") String action,
                         JsonVertex jsonVertex) {
        LOG.debug("Graph [{}] update vertex: {}", graph, jsonVertex);
        checkUpdatingBody(jsonVertex);

        Id id = checkAndParseVertexId(idValue);
        // Parse action param
        boolean append = checkAndParseAction(action);

        Vortex g = graph(manager, graph);
        VortexVertex vertex = (VortexVertex) g.vertex(id);
        VertexLabel vertexLabel = vertex.schemaLabel();

        for (String key : jsonVertex.properties.keySet()) {
            PropertyKey pkey = g.propertyKey(key);
            E.checkArgument(vertexLabel.properties().contains(pkey.id()),
                            "Can't update property for vertex '%s' because " +
                            "there is no property key '%s' in its vertex label",
                            id, key);
        }

        commit(g, () -> updateProperties(vertex, jsonVertex, append));

        return manager.serializer(g).writeVertex(vertex);
    }

    @GET
    @Timed
    @Compress
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$graph $action=vertex_read"})
    public String list(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @QueryParam("label") String label,
                       @QueryParam("properties") String properties,
                       @QueryParam("keep_start_p")
                       @DefaultValue("false") boolean keepStartP,
                       @QueryParam("offset") @DefaultValue("0") long offset,
                       @QueryParam("page") String page,
                       @QueryParam("limit") @DefaultValue("100") long limit) {
        LOG.debug("Graph [{}] query vertices by label: {}, properties: {}, " +
                  "offset: {}, page: {}, limit: {}",
                  graph, label, properties, offset, page, limit);

        Map<String, Object> props = parseProperties(properties);
        if (page != null) {
            E.checkArgument(offset == 0,
                            "Not support querying vertices based on paging " +
                            "and offset together");
        }

        Vortex g = graph(manager, graph);

        GraphTraversal<Vertex, Vertex> traversal = g.traversal().V();
        if (label != null) {
            traversal = traversal.hasLabel(label);
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
            return manager.serializer(g).writeVertices(traversal,
                                                       page != null);
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
    @RolesAllowed({"admin", "$owner=$graph $action=vertex_read"})
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @PathParam("id") String idValue) {
        LOG.debug("Graph [{}] get vertex by id '{}'", graph, idValue);

        Id id = checkAndParseVertexId(idValue);
        Vortex g = graph(manager, graph);
        try {
            Vertex vertex = g.vertex(id);
            return manager.serializer(g).writeVertex(vertex);
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
    @RolesAllowed({"admin", "$owner=$graph $action=vertex_delete"})
    public void delete(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @PathParam("id") String idValue,
                       @QueryParam("label") String label) {
        LOG.debug("Graph [{}] remove vertex by id '{}'", graph, idValue);

        Id id = checkAndParseVertexId(idValue);
        Vortex g = graph(manager, graph);
        commit(g, () -> {
            try {
                g.removeVertex(label, id);
            } catch (NotFoundException e) {
                throw new IllegalArgumentException(String.format(
                          "No such vertex with id: '%s', %s", id, e));
            } catch (NoSuchElementException e) {
                throw new IllegalArgumentException(String.format(
                          "No such vertex with id: '%s'", id));
            }
        });
    }

    public static Id checkAndParseVertexId(String idValue) {
        if (idValue == null) {
            return null;
        }
        boolean uuid = idValue.startsWith("U\"");
        if (uuid) {
            idValue = idValue.substring(1);
        }
        try {
            Object id = JsonUtil.fromJson(idValue, Object.class);
            return uuid ? Text.uuid((String) id) : VortexVertex.getIdValue(id);
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format(
                      "The vertex id must be formatted as Number/String/UUID" +
                      ", but got '%s'", idValue));
        }
    }

    private static void checkBatchSize(VortexConfig config,
                                       List<JsonVertex> vertices) {
        int max = config.get(ServerOptions.MAX_VERTICES_PER_BATCH);
        if (vertices.size() > max) {
            throw new IllegalArgumentException(String.format(
                      "Too many vertices for one time post, " +
                      "the maximum number is '%s'", max));
        }
        if (vertices.size() == 0) {
            throw new IllegalArgumentException(
                      "The number of vertices can't be 0");
        }
    }

    private static Id getVertexId(Vortex g, JsonVertex vertex) {
        VertexLabel vertexLabel = g.vertexLabel(vertex.label);
        String labelId = vertexLabel.id().asString();
        IdStrategy idStrategy = vertexLabel.idStrategy();
        E.checkArgument(idStrategy != IdStrategy.AUTOMATIC,
                        "Automatic Id strategy is not supported now");

        if (idStrategy == IdStrategy.PRIMARY_KEY) {
            List<Id> pkIds = vertexLabel.primaryKeys();
            List<Object> pkValues = new ArrayList<>(pkIds.size());
            for (Id pkId : pkIds) {
                String propertyKey = g.propertyKey(pkId).name();
                Object propertyValue = vertex.properties.get(propertyKey);
                E.checkArgument(propertyValue != null,
                                "The value of primary key '%s' can't be null",
                                propertyKey);
                pkValues.add(propertyValue);
            }

            String value = ConditionQuery.concatValues(pkValues);
            return SplicingIdGenerator.splicing(labelId, value);
        } else if (idStrategy == IdStrategy.CUSTOMIZE_UUID) {
            return Text.uuid(String.valueOf(vertex.id));
        } else {
            assert idStrategy == IdStrategy.CUSTOMIZE_NUMBER ||
                   idStrategy == IdStrategy.CUSTOMIZE_STRING;
            return VortexVertex.getIdValue(vertex.id);
        }
    }

    private static class BatchVertexRequest {

        @JsonProperty("vertices")
        public List<JsonVertex> jsonVertices;
        @JsonProperty("update_strategies")
        public Map<String, UpdateStrategy> updateStrategies;
        @JsonProperty("create_if_not_exist")
        public boolean createIfNotExist = true;

        private static void checkUpdate(BatchVertexRequest req) {
            E.checkArgumentNotNull(req, "BatchVertexRequest can't be null");
            E.checkArgumentNotNull(req.jsonVertices,
                                   "Parameter 'vertices' can't be null");
            E.checkArgument(req.updateStrategies != null &&
                            !req.updateStrategies.isEmpty(),
                            "Parameter 'update_strategies' can't be empty");
            E.checkArgument(req.createIfNotExist == true,
                            "Parameter 'create_if_not_exist' " +
                            "dose not support false now");
        }

        @Override
        public String toString() {
            return String.format("BatchVertexRequest{jsonVertices=%s," +
                                 "updateStrategies=%s,createIfNotExist=%s}",
                                 this.jsonVertices, this.updateStrategies,
                                 this.createIfNotExist);
        }
    }

    private static class JsonVertex extends JsonElement {

        @Override
        public void checkCreate(boolean isBatch) {
            this.checkUpdate();
        }

        @Override
        public void checkUpdate() {
            E.checkArgumentNotNull(this.properties,
                                   "The properties of vertex can't be null");

            for (Map.Entry<String, Object> e : this.properties.entrySet()) {
                String key = e.getKey();
                Object value = e.getValue();
                E.checkArgumentNotNull(value, "Not allowed to set value of " +
                                       "property '%s' to null for vertex '%s'",
                                       key, this.id);
            }
        }

        @Override
        public Object[] properties() {
            Object[] props = API.properties(this.properties);
            int newSize = props.length;
            int appendIndex = newSize;
            if (this.label != null) {
                newSize += 2;
            }
            if (this.id != null) {
                newSize += 2;
            }
            if (newSize == props.length) {
                return props;
            }

            Object[] newProps = Arrays.copyOf(props, newSize);
            if (this.label != null) {
                newProps[appendIndex++] = T.label;
                newProps[appendIndex++] = this.label;
            }
            if (this.id != null) {
                newProps[appendIndex++] = T.id;
                newProps[appendIndex++] = this.id;
            }
            return newProps;
        }

        @Override
        public String toString() {
            return String.format("JsonVertex{label=%s, properties=%s}",
                                 this.label, this.properties);
        }
    }
}
