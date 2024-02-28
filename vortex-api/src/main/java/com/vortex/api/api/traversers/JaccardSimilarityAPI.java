
package com.vortex.api.api.traversers;

import com.vortex.vortexdb.Vortex;
import com.vortex.api.api.graph.EdgeAPI;
import com.vortex.api.api.graph.VertexAPI;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.api.core.GraphManager;
import com.vortex.api.server.RestServer;
import com.vortex.vortexdb.structure.VortexVertex;
import com.vortex.vortexdb.traversal.algorithm.JaccardSimilarTraverser;
import com.vortex.vortexdb.traversal.algorithm.steps.EdgeStep;
import com.vortex.vortexdb.type.define.Directions;
import com.vortex.common.util.E;
import com.vortex.vortexdb.util.JsonUtil;
import com.vortex.common.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;

import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import java.util.Map;

import static com.vortex.vortexdb.traversal.algorithm.VortexTraverser.*;

@Path("graphs/{graph}/traversers/jaccardsimilarity")
@Singleton
public class JaccardSimilarityAPI extends TraverserAPI {

    private static final Logger LOG = Log.logger(RestServer.class);

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @QueryParam("vertex") String vertex,
                      @QueryParam("other") String other,
                      @QueryParam("direction") String direction,
                      @QueryParam("label") String edgeLabel,
                      @QueryParam("max_degree")
                      @DefaultValue(DEFAULT_MAX_DEGREE) long maxDegree) {
        LOG.debug("Graph [{}] get jaccard similarity between '{}' and '{}' " +
                  "with direction {}, edge label {} and max degree '{}'",
                  graph, vertex, other, direction, edgeLabel, maxDegree);

        Id sourceId = VertexAPI.checkAndParseVertexId(vertex);
        Id targetId = VertexAPI.checkAndParseVertexId(other);
        Directions dir = Directions.convert(EdgeAPI.parseDirection(direction));

        Vortex g = graph(manager, graph);
        double similarity;
        try (JaccardSimilarTraverser traverser =
                                     new JaccardSimilarTraverser(g)) {
             similarity = traverser.jaccardSimilarity(sourceId, targetId, dir,
                                                      edgeLabel, maxDegree);
        }
        return JsonUtil.toJson(ImmutableMap.of("jaccard_similarity",
                                               similarity));
    }

    @POST
    @Timed
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String post(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       Request request) {
        E.checkArgumentNotNull(request, "The request body can't be null");
        E.checkArgumentNotNull(request.vertex,
                               "The source vertex of request can't be null");
        E.checkArgument(request.step != null,
                        "The steps of request can't be null");
        E.checkArgument(request.top >= 0,
                        "The top must be >= 0, but got: %s", request.top);

        LOG.debug("Graph [{}] get jaccard similars from source vertex '{}', " +
                  "with step '{}', top '{}' and capacity '{}'",
                  graph, request.vertex, request.step,
                  request.top, request.capacity);

        Vortex g = graph(manager, graph);
        Id sourceId = VortexVertex.getIdValue(request.vertex);

        EdgeStep step = step(g, request.step);

        Map<Id, Double> results;
        try (JaccardSimilarTraverser traverser =
                                     new JaccardSimilarTraverser(g)) {
            results = traverser.jaccardSimilars(sourceId, step, request.top,
                                                request.capacity);
        }
        return manager.serializer(g).writeMap(results);
    }

    private static class Request {

        @JsonProperty("vertex")
        public Object vertex;
        @JsonProperty("step")
        public TraverserAPI.Step step;
        @JsonProperty("top")
        public int top = Integer.parseInt(DEFAULT_LIMIT);
        @JsonProperty("capacity")
        public long capacity = Long.parseLong(DEFAULT_CAPACITY);

        @Override
        public String toString() {
            return String.format("Request{vertex=%s,step=%s,top=%s," +
                                 "capacity=%s}", this.vertex, this.step,
                                 this.top, this.capacity);
        }
    }
}
