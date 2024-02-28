
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
import com.vortex.vortexdb.traversal.algorithm.KneighborTraverser;
import com.vortex.vortexdb.traversal.algorithm.records.KneighborRecords;
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
import javax.ws.rs.core.Context;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.vortex.vortexdb.traversal.algorithm.VortexTraverser.DEFAULT_ELEMENTS_LIMIT;
import static com.vortex.vortexdb.traversal.algorithm.VortexTraverser.DEFAULT_MAX_DEGREE;

@Path("graphs/{graph}/traversers/kneighbor")
@Singleton
public class KneighborAPI extends TraverserAPI {

    private static final Logger LOG = Log.logger(RestServer.class);

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @QueryParam("source") String sourceV,
                      @QueryParam("direction") String direction,
                      @QueryParam("label") String edgeLabel,
                      @QueryParam("max_depth") int depth,
                      @QueryParam("max_degree")
                      @DefaultValue(DEFAULT_MAX_DEGREE) long maxDegree,
                      @QueryParam("limit")
                      @DefaultValue(DEFAULT_ELEMENTS_LIMIT) long limit) {
        LOG.debug("Graph [{}] get k-neighbor from '{}' with " +
                  "direction '{}', edge label '{}', max depth '{}', " +
                  "max degree '{}' and limit '{}'",
                  graph, sourceV, direction, edgeLabel, depth,
                  maxDegree, limit);

        Id source = VertexAPI.checkAndParseVertexId(sourceV);
        Directions dir = Directions.convert(EdgeAPI.parseDirection(direction));

        Vortex g = graph(manager, graph);

        Set<Id> ids;
        try (KneighborTraverser traverser = new KneighborTraverser(g)) {
            ids = traverser.kneighbor(source, dir, edgeLabel,
                                      depth, maxDegree, limit);
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

        LOG.debug("Graph [{}] get customized kneighbor from source vertex " +
                  "'{}', with step '{}', limit '{}', count_only '{}', " +
                  "with_vertex '{}' and with_path '{}'",
                  graph, request.source, request.step, request.limit,
                  request.countOnly, request.withVertex, request.withPath);

        Vortex g = graph(manager, graph);
        Id sourceId = VortexVertex.getIdValue(request.source);

        EdgeStep step = step(g, request.step);

        KneighborRecords results;
        try (KneighborTraverser traverser = new KneighborTraverser(g)) {
            results = traverser.customizedKneighbor(sourceId, step,
                                                    request.maxDepth,
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
        return manager.serializer(g).writeNodesWithPath("kneighbor", neighbors,
                                                        size, paths, iter);
    }

    private static class Request {

        @JsonProperty("source")
        public Object source;
        @JsonProperty("step")
        public TraverserAPI.Step step;
        @JsonProperty("max_depth")
        public int maxDepth;
        @JsonProperty("limit")
        public long limit = Long.parseLong(DEFAULT_ELEMENTS_LIMIT);
        @JsonProperty("count_only")
        public boolean countOnly = false;
        @JsonProperty("with_vertex")
        public boolean withVertex = false;
        @JsonProperty("with_path")
        public boolean withPath = false;

        @Override
        public String toString() {
            return String.format("PathRequest{source=%s,step=%s,maxDepth=%s" +
                                 "limit=%s,countOnly=%s,withVertex=%s," +
                                 "withPath=%s}", this.source, this.step,
                                 this.maxDepth, this.limit, this.countOnly,
                                 this.withVertex, this.withPath);
        }
    }
}
