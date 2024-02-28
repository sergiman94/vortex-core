
package com.vortex.api.api.traversers;

import com.vortex.vortexdb.Vortex;
import com.vortex.api.api.API;
import com.vortex.vortexdb.backend.query.QueryResults;
import com.vortex.api.core.GraphManager;
import com.vortex.api.server.RestServer;
import com.vortex.vortexdb.traversal.algorithm.FusiformSimilarityTraverser;
import com.vortex.vortexdb.traversal.algorithm.FusiformSimilarityTraverser.SimilarsMap;
import com.vortex.vortexdb.type.define.Directions;
import com.vortex.common.util.E;
import com.vortex.common.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.slf4j.Logger;

import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import java.util.Iterator;

import static com.vortex.vortexdb.traversal.algorithm.VortexTraverser.*;

@Path("graphs/{graph}/traversers/fusiformsimilarity")
@Singleton
public class FusiformSimilarityAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String post(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       FusiformSimilarityRequest request) {
        E.checkArgumentNotNull(request, "The fusiform similarity " +
                               "request body can't be null");
        E.checkArgumentNotNull(request.sources,
                               "The sources of fusiform similarity " +
                               "request can't be null");
        if (request.direction == null) {
            request.direction = Directions.BOTH;
        }
        E.checkArgument(request.minNeighbors > 0,
                        "The min neighbor count must be > 0, but got: %s",
                        request.minNeighbors);
        E.checkArgument(request.maxDegree > 0L || request.maxDegree == NO_LIMIT,
                        "The max degree of request must be > 0 or == -1, " +
                        "but got: %s", request.maxDegree);
        E.checkArgument(request.alpha > 0 && request.alpha <= 1.0,
                        "The alpha of request must be in range (0, 1], " +
                        "but got '%s'", request.alpha);
        E.checkArgument(request.minSimilars >= 1,
                        "The min similar count of request must be >= 1, " +
                        "but got: %s", request.minSimilars);
        E.checkArgument(request.top >= 0,
                        "The top must be >= 0, but got: %s", request.top);

        LOG.debug("Graph [{}] get fusiform similars from '{}' with " +
                  "direction '{}', edge label '{}', min neighbor count '{}', " +
                  "alpha '{}', min similar count '{}', group property '{}' " +
                  "and min group count '{}'",
                  graph, request.sources, request.direction, request.label,
                  request.minNeighbors, request.alpha, request.minSimilars,
                  request.groupProperty, request.minGroups);

        Vortex g = graph(manager, graph);
        Iterator<Vertex> sources = request.sources.vertices(g);
        E.checkArgument(sources != null && sources.hasNext(),
                        "The source vertices can't be empty");

        FusiformSimilarityTraverser traverser =
                                    new FusiformSimilarityTraverser(g);
        SimilarsMap result = traverser.fusiformSimilarity(
                             sources, request.direction, request.label,
                             request.minNeighbors, request.alpha,
                             request.minSimilars, request.top,
                             request.groupProperty, request.minGroups,
                             request.maxDegree, request.capacity,
                             request.limit, request.withIntermediary);

        CloseableIterator.closeIterator(sources);

        Iterator<Vertex> iterator = QueryResults.emptyIterator();
        if (request.withVertex && !result.isEmpty()) {
            iterator = g.vertices(result.vertices().toArray());
        }
        return manager.serializer(g).writeSimilars(result, iterator);
    }

    private static class FusiformSimilarityRequest {

        @JsonProperty("sources")
        public Vertices sources;
        @JsonProperty("label")
        public String label;
        @JsonProperty("direction")
        public Directions direction;
        @JsonProperty("min_neighbors")
        public int minNeighbors;
        @JsonProperty("alpha")
        public double alpha;
        @JsonProperty("min_similars")
        public int minSimilars = 1;
        @JsonProperty("top")
        public int top;
        @JsonProperty("group_property")
        public String groupProperty;
        @JsonProperty("min_groups")
        public int minGroups;
        @JsonProperty("max_degree")
        public long maxDegree = Long.parseLong(DEFAULT_MAX_DEGREE);
        @JsonProperty("capacity")
        public long capacity = Long.parseLong(DEFAULT_CAPACITY);
        @JsonProperty("limit")
        public long limit = Long.parseLong(DEFAULT_PATHS_LIMIT);
        @JsonProperty("with_intermediary")
        public boolean withIntermediary = false;
        @JsonProperty("with_vertex")
        public boolean withVertex = false;

        @Override
        public String toString() {
            return String.format("FusiformSimilarityRequest{sources=%s," +
                                 "label=%s,direction=%s,minNeighbors=%s," +
                                 "alpha=%s,minSimilars=%s,top=%s," +
                                 "groupProperty=%s,minGroups=%s," +
                                 "maxDegree=%s,capacity=%s,limit=%s," +
                                 "withIntermediary=%s,withVertex=%s}",
                                 this.sources, this.label, this.direction,
                                 this.minNeighbors, this.alpha,
                                 this.minSimilars, this.top,
                                 this.groupProperty, this.minGroups,
                                 this.maxDegree, this.capacity, this.limit,
                                 this.withIntermediary, this.withVertex);
        }
    }
}
