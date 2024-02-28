
package com.vortex.api.api.traversers;

import com.vortex.vortexdb.Vortex;
import com.vortex.api.api.API;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.query.QueryResults;
import com.vortex.api.core.GraphManager;
import com.vortex.api.server.RestServer;
import com.vortex.vortexdb.traversal.algorithm.CustomizePathsTraverser;
import com.vortex.vortexdb.traversal.algorithm.VortexTraverser;
import com.vortex.vortexdb.traversal.algorithm.steps.WeightedEdgeStep;
import com.vortex.vortexdb.type.define.Directions;
import com.vortex.common.util.E;
import com.vortex.common.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import java.util.*;

import static com.vortex.vortexdb.traversal.algorithm.VortexTraverser.*;

@Path("graphs/{graph}/traversers/customizedpaths")
@Singleton
public class CustomizedPathsAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String post(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       PathRequest request) {
        E.checkArgumentNotNull(request, "The path request body can't be null");
        E.checkArgumentNotNull(request.sources,
                               "The sources of path request can't be null");
        E.checkArgument(request.steps != null && !request.steps.isEmpty(),
                        "The steps of path request can't be empty");
        if (request.sortBy == null) {
            request.sortBy = SortBy.NONE;
        }

        LOG.debug("Graph [{}] get customized paths from source vertex '{}', " +
                  "with steps '{}', sort by '{}', capacity '{}', limit '{}' " +
                  "and with_vertex '{}'", graph, request.sources, request.steps,
                  request.sortBy, request.capacity, request.limit,
                  request.withVertex);

        Vortex g = graph(manager, graph);
        Iterator<Vertex> sources = request.sources.vertices(g);
        List<WeightedEdgeStep> steps = step(g, request);
        boolean sorted = request.sortBy != SortBy.NONE;

        CustomizePathsTraverser traverser = new CustomizePathsTraverser(g);
        List<VortexTraverser.Path> paths;
        paths = traverser.customizedPaths(sources, steps, sorted,
                                          request.capacity, request.limit);

        if (sorted) {
            boolean incr = request.sortBy == SortBy.INCR;
            paths = CustomizePathsTraverser.topNPath(paths, incr,
                                                     request.limit);
        }

        if (!request.withVertex) {
            return manager.serializer(g).writePaths("paths", paths, false);
        }

        Set<Id> ids = new HashSet<>();
        for (VortexTraverser.Path p : paths) {
            ids.addAll(p.vertices());
        }
        Iterator<Vertex> iter = QueryResults.emptyIterator();
        if (!ids.isEmpty()) {
            iter = g.vertices(ids.toArray());
        }
        return manager.serializer(g).writePaths("paths", paths, false, iter);
    }

    private static List<WeightedEdgeStep> step(Vortex graph,
                                               PathRequest req) {
        int stepSize = req.steps.size();
        List<WeightedEdgeStep> steps = new ArrayList<>(stepSize);
        for (Step step : req.steps) {
            steps.add(step.jsonToStep(graph));
        }
        return steps;
    }

    private static class PathRequest {

        @JsonProperty("sources")
        public Vertices sources;
        @JsonProperty("steps")
        public List<Step> steps;
        @JsonProperty("sort_by")
        public SortBy sortBy;
        @JsonProperty("capacity")
        public long capacity = Long.parseLong(DEFAULT_CAPACITY);
        @JsonProperty("limit")
        public long limit = Long.parseLong(DEFAULT_PATHS_LIMIT);
        @JsonProperty("with_vertex")
        public boolean withVertex = false;

        @Override
        public String toString() {
            return String.format("PathRequest{sourceVertex=%s,steps=%s," +
                                 "sortBy=%s,capacity=%s,limit=%s," +
                                 "withVertex=%s}", this.sources, this.steps,
                                 this.sortBy, this.capacity, this.limit,
                                 this.withVertex);
        }
    }

    private static class Step {

        @JsonProperty("direction")
        public Directions direction;
        @JsonProperty("labels")
        public List<String> labels;
        @JsonProperty("properties")
        public Map<String, Object> properties;
        @JsonAlias("degree")
        @JsonProperty("max_degree")
        public long maxDegree = Long.parseLong(DEFAULT_MAX_DEGREE);
        @JsonProperty("skip_degree")
        public long skipDegree = 0L;
        @JsonProperty("weight_by")
        public String weightBy;
        @JsonProperty("default_weight")
        public double defaultWeight = Double.parseDouble(DEFAULT_WEIGHT);
        @JsonProperty("sample")
        public long sample = Long.parseLong(DEFAULT_SAMPLE);

        @Override
        public String toString() {
            return String.format("Step{direction=%s,labels=%s,properties=%s," +
                                 "maxDegree=%s,skipDegree=%s," +
                                 "weightBy=%s,defaultWeight=%s,sample=%s}",
                                 this.direction, this.labels, this.properties,
                                 this.maxDegree, this.skipDegree,
                                 this.weightBy, this.defaultWeight,
                                 this.sample);
        }

        private WeightedEdgeStep jsonToStep(Vortex g) {
            return new WeightedEdgeStep(g, this.direction, this.labels,
                                        this.properties, this.maxDegree,
                                        this.skipDegree, this.weightBy,
                                        this.defaultWeight, this.sample);
        }
    }

    private enum SortBy {
        INCR,
        DECR,
        NONE
    }
}
