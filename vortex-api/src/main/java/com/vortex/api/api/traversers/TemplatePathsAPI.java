
package com.vortex.api.api.traversers;

import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.query.QueryResults;
import com.vortex.api.core.GraphManager;
import com.vortex.api.server.RestServer;
import com.vortex.vortexdb.traversal.algorithm.VortexTraverser;
import com.vortex.vortexdb.traversal.algorithm.TemplatePathsTraverser;
import com.vortex.vortexdb.traversal.algorithm.steps.RepeatEdgeStep;
import com.vortex.common.util.E;
import com.vortex.common.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import java.util.*;

import static com.vortex.vortexdb.traversal.algorithm.VortexTraverser.DEFAULT_CAPACITY;
import static com.vortex.vortexdb.traversal.algorithm.VortexTraverser.DEFAULT_PATHS_LIMIT;

@Path("graphs/{graph}/traversers/templatepaths")
@Singleton
public class TemplatePathsAPI extends TraverserAPI {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String post(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       Request request) {
        E.checkArgumentNotNull(request, "The request body can't be null");
        E.checkArgumentNotNull(request.sources,
                               "The sources of request can't be null");
        E.checkArgumentNotNull(request.targets,
                               "The targets of request can't be null");
        E.checkArgument(request.steps != null && !request.steps.isEmpty(),
                        "The steps of request can't be empty");

        LOG.debug("Graph [{}] get template paths from source vertices '{}', " +
                  "target vertices '{}', with steps '{}', " +
                  "capacity '{}', limit '{}' and with_vertex '{}'",
                  graph, request.sources, request.targets, request.steps,
                  request.capacity, request.limit, request.withVertex);

        Vortex g = graph(manager, graph);
        Iterator<Vertex> sources = request.sources.vertices(g);
        Iterator<Vertex> targets = request.targets.vertices(g);
        List<RepeatEdgeStep> steps = steps(g, request.steps);

        TemplatePathsTraverser traverser = new TemplatePathsTraverser(g);
        Set<VortexTraverser.Path> paths;
        paths = traverser.templatePaths(sources, targets, steps,
                                        request.withRing, request.capacity,
                                        request.limit);

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

    private static List<RepeatEdgeStep> steps(Vortex g,
                                              List<TemplatePathStep> steps) {
        List<RepeatEdgeStep> edgeSteps = new ArrayList<>(steps.size());
        for (TemplatePathStep step : steps) {
            edgeSteps.add(repeatEdgeStep(g, step));
        }
        return edgeSteps;
    }

    private static RepeatEdgeStep repeatEdgeStep(Vortex graph,
                                                 TemplatePathStep step) {
        return new RepeatEdgeStep(graph, step.direction, step.labels,
                                  step.properties, step.maxDegree,
                                  step.skipDegree, step.maxTimes);
    }

    private static class Request {

        @JsonProperty("sources")
        public Vertices sources;
        @JsonProperty("targets")
        public Vertices targets;
        @JsonProperty("steps")
        public List<TemplatePathStep> steps;
        @JsonProperty("with_ring")
        public boolean withRing = false;
        @JsonProperty("capacity")
        public long capacity = Long.parseLong(DEFAULT_CAPACITY);
        @JsonProperty("limit")
        public long limit = Long.parseLong(DEFAULT_PATHS_LIMIT);
        @JsonProperty("with_vertex")
        public boolean withVertex = false;

        @Override
        public String toString() {
            return String.format("TemplatePathsRequest{sources=%s,targets=%s," +
                                 "steps=%s,withRing=%s,capacity=%s,limit=%s," +
                                 "withVertex=%s}",
                                 this.sources, this.targets, this.steps,
                                 this.withRing, this.capacity, this.limit,
                                 this.withVertex);
        }
    }

    protected static class TemplatePathStep extends Step {

        @JsonProperty("max_times")
        public int maxTimes = 1;

        @Override
        public String toString() {
            return String.format("TemplatePathStep{direction=%s,labels=%s," +
                                 "properties=%s,maxDegree=%s,skipDegree=%s," +
                                 "maxTimes=%s}",
                                 this.direction, this.labels, this.properties,
                                 this.maxDegree, this.skipDegree,
                                 this.maxTimes);
        }
    }
}
