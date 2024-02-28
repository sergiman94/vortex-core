
package com.vortex.api.api.traversers;

import com.vortex.vortexdb.Vortex;
import com.vortex.api.api.API;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.query.QueryResults;
import com.vortex.api.core.GraphManager;
import com.vortex.api.server.RestServer;
import com.vortex.vortexdb.traversal.algorithm.CustomizedCrosspointsTraverser;
import com.vortex.vortexdb.traversal.algorithm.VortexTraverser;
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

@Path("graphs/{graph}/traversers/customizedcrosspoints")
@Singleton
public class CustomizedCrosspointsAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String post(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       CrosspointsRequest request) {
        E.checkArgumentNotNull(request,
                               "The crosspoints request body can't be null");
        E.checkArgumentNotNull(request.sources,
                               "The sources of crosspoints request " +
                               "can't be null");
        E.checkArgument(request.pathPatterns != null &&
                        !request.pathPatterns.isEmpty(),
                        "The steps of crosspoints request can't be empty");

        LOG.debug("Graph [{}] get customized crosspoints from source vertex " +
                  "'{}', with path_pattern '{}', with_path '{}', with_vertex " +
                  "'{}', capacity '{}' and limit '{}'", graph, request.sources,
                  request.pathPatterns, request.withPath, request.withVertex,
                  request.capacity, request.limit);

        Vortex g = graph(manager, graph);
        Iterator<Vertex> sources = request.sources.vertices(g);
        List<CustomizedCrosspointsTraverser.PathPattern> patterns;
        patterns = pathPatterns(g, request);

        CustomizedCrosspointsTraverser traverser =
                                       new CustomizedCrosspointsTraverser(g);
        CustomizedCrosspointsTraverser.CrosspointsPaths paths;
        paths = traverser.crosspointsPaths(sources, patterns, request.capacity,
                                           request.limit);
        Iterator<Vertex> iter = QueryResults.emptyIterator();
        if (!request.withVertex) {
            return manager.serializer(g).writeCrosspoints(paths, iter,
                                                          request.withPath);
        }
        Set<Id> ids = new HashSet<>();
        if (request.withPath) {
            for (VortexTraverser.Path p : paths.paths()) {
                ids.addAll(p.vertices());
            }
        } else {
            ids = paths.crosspoints();
        }
        if (!ids.isEmpty()) {
            iter = g.vertices(ids.toArray());
        }
        return manager.serializer(g).writeCrosspoints(paths, iter,
                                                      request.withPath);
    }

    private static List<CustomizedCrosspointsTraverser.PathPattern>
                   pathPatterns(Vortex graph, CrosspointsRequest request) {
        int stepSize = request.pathPatterns.size();
        List<CustomizedCrosspointsTraverser.PathPattern> pathPatterns;
        pathPatterns = new ArrayList<>(stepSize);
        for (PathPattern pattern : request.pathPatterns) {
            CustomizedCrosspointsTraverser.PathPattern pathPattern;
            pathPattern = new CustomizedCrosspointsTraverser.PathPattern();
            for (Step step : pattern.steps) {
                pathPattern.add(step.jsonToStep(graph));
            }
            pathPatterns.add(pathPattern);
        }
        return pathPatterns;
    }

    private static class CrosspointsRequest {

        @JsonProperty("sources")
        public Vertices sources;
        @JsonProperty("path_patterns")
        public List<PathPattern> pathPatterns;
        @JsonProperty("capacity")
        public long capacity = Long.parseLong(DEFAULT_CAPACITY);
        @JsonProperty("limit")
        public long limit = Long.parseLong(DEFAULT_PATHS_LIMIT);
        @JsonProperty("with_path")
        public boolean withPath = false;
        @JsonProperty("with_vertex")
        public boolean withVertex = false;

        @Override
        public String toString() {
            return String.format("CrosspointsRequest{sourceVertex=%s," +
                                 "pathPatterns=%s,withPath=%s,withVertex=%s," +
                                 "capacity=%s,limit=%s}", this.sources,
                                 this.pathPatterns, this.withPath,
                                 this.withVertex, this.capacity, this.limit);
        }
    }

    private static class PathPattern {

        @JsonProperty("steps")
        public List<Step> steps;

        @Override
        public String toString() {
            return String.format("PathPattern{steps=%s", this.steps);
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

        @Override
        public String toString() {
            return String.format("Step{direction=%s,labels=%s,properties=%s," +
                                 "maxDegree=%s,skipDegree=%s}",
                                 this.direction, this.labels, this.properties,
                                 this.maxDegree, this.skipDegree);
        }

        private CustomizedCrosspointsTraverser.Step jsonToStep(Vortex g) {
            return new CustomizedCrosspointsTraverser.Step(g, this.direction,
                                                           this.labels,
                                                           this.properties,
                                                           this.maxDegree,
                                                           this.skipDegree);
        }
    }
}
