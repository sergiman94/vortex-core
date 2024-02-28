
package com.vortex.api.api.traversers;

import com.vortex.vortexdb.Vortex;
import com.vortex.api.api.graph.EdgeAPI;
import com.vortex.api.api.graph.VertexAPI;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.query.Query;
import com.vortex.vortexdb.backend.query.QueryResults;
import com.vortex.api.core.GraphManager;
import com.vortex.api.server.RestServer;
import com.vortex.vortexdb.structure.VortexVertex;
import com.vortex.vortexdb.traversal.algorithm.VortexTraverser;
import com.vortex.vortexdb.traversal.algorithm.KoutTraverser;
import com.vortex.vortexdb.traversal.algorithm.records.KoutRecords;
import com.vortex.vortexdb.traversal.algorithm.steps.EdgeStep;
import com.vortex.vortexdb.type.define.Directions;
import com.vortex.common.util.E;
import com.vortex.common.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.vortex.vortexdb.traversal.algorithm.VortexTraverser.*;

@Path("graphs/{graph}/traversers/kout")
@Singleton
public class KoutAPI extends TraverserAPI {

    private static final Logger LOG = Log.logger(RestServer.class);

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @QueryParam("source") String source,
                      @QueryParam("direction") String direction,
                      @QueryParam("label") String edgeLabel,
                      @QueryParam("max_depth") int depth,
                      @QueryParam("nearest")
                      @DefaultValue("true")  boolean nearest,
                      @QueryParam("max_degree")
                      @DefaultValue(DEFAULT_MAX_DEGREE) long maxDegree,
                      @QueryParam("capacity")
                      @DefaultValue(DEFAULT_CAPACITY) long capacity,
                      @QueryParam("limit")
                      @DefaultValue(DEFAULT_ELEMENTS_LIMIT) long limit) {
        LOG.debug("Graph [{}] get k-out from '{}' with " +
                  "direction '{}', edge label '{}', max depth '{}', nearest " +
                  "'{}', max degree '{}', capacity '{}' and limit '{}'",
                  graph, source, direction, edgeLabel, depth, nearest,
                  maxDegree, capacity, limit);

        Id sourceId = VertexAPI.checkAndParseVertexId(source);
        Directions dir = Directions.convert(EdgeAPI.parseDirection(direction));

        Vortex g = graph(manager, graph);

        Set<Id> ids;
        try (KoutTraverser traverser = new KoutTraverser(g)) {
            ids = traverser.kout(sourceId, dir, edgeLabel, depth,
                                 nearest, maxDegree, capacity, limit);
        }
        return manager.serializer(g).writeList("vertices", ids);
    }

    @POST
    @Timed
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String post(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       Request request) {
        E.checkArgumentNotNull(request, "The request body can't be null");
        E.checkArgumentNotNull(request.source,
                               "The source of request can't be null");
        E.checkArgument(request.step != null,
                        "The steps of request can't be null");
        if (request.countOnly) {
            E.checkArgument(!request.withVertex && !request.withPath,
                            "Can't return vertex or path when count only");
        }

        LOG.debug("Graph [{}] get customized kout from source vertex '{}', " +
                  "with step '{}', max_depth '{}', nearest '{}', " +
                  "count_only '{}', capacity '{}', limit '{}', " +
                  "with_vertex '{}' and with_path '{}'",
                  graph, request.source, request.step, request.maxDepth,
                  request.nearest, request.countOnly, request.capacity,
                  request.limit, request.withVertex, request.withPath);

        Vortex g = graph(manager, graph);
        Id sourceId = VortexVertex.getIdValue(request.source);

        EdgeStep step = step(g, request.step);

        KoutRecords results;
        try (KoutTraverser traverser = new KoutTraverser(g)) {
            results = traverser.customizedKout(sourceId, step,
                                               request.maxDepth,
                                               request.nearest,
                                               request.capacity,
                                               request.limit);
        }

        long size = results.size();
        if (request.limit != Query.NO_LIMIT && size > request.limit) {
            size = request.limit;
        }
        List<Id> neighbors = request.countOnly ?
                             ImmutableList.of() : results.ids(request.limit);

        VortexTraverser.PathSet paths = new VortexTraverser.PathSet();
        if (request.withPath) {
            paths.addAll(results.paths(request.limit));
        }
        Iterator<Vertex> iter = QueryResults.emptyIterator();
        if (request.withVertex && !request.countOnly) {
            Set<Id> ids = new HashSet<>(neighbors);
            if (request.withPath) {
                for (VortexTraverser.Path p : paths) {
                    ids.addAll(p.vertices());
                }
            }
            if (!ids.isEmpty()) {
                iter = g.vertices(ids.toArray());
            }
        }
        return manager.serializer(g).writeNodesWithPath("kout", neighbors,
                                                        size, paths, iter);
    }

    private static class Request {

        @JsonProperty("source")
        public Object source;
        @JsonProperty("step")
        public TraverserAPI.Step step;
        @JsonProperty("max_depth")
        public int maxDepth;
        @JsonProperty("nearest")
        public boolean nearest = true;
        @JsonProperty("count_only")
        public boolean countOnly = false;
        @JsonProperty("capacity")
        public long capacity = Long.parseLong(DEFAULT_CAPACITY);
        @JsonProperty("limit")
        public long limit = Long.parseLong(DEFAULT_ELEMENTS_LIMIT);
        @JsonProperty("with_vertex")
        public boolean withVertex = false;
        @JsonProperty("with_path")
        public boolean withPath = false;

        @Override
        public String toString() {
            return String.format("KoutRequest{source=%s,step=%s,maxDepth=%s" +
                                 "nearest=%s,countOnly=%s,capacity=%s," +
                                 "limit=%s,withVertex=%s,withPath=%s}",
                                 this.source, this.step, this.maxDepth,
                                 this.nearest, this.countOnly, this.capacity,
                                 this.limit, this.withVertex, this.withPath);
        }
    }
}
