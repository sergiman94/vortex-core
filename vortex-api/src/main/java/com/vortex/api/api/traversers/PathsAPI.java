
package com.vortex.api.api.traversers;

import com.vortex.vortexdb.Vortex;
import com.vortex.api.api.graph.EdgeAPI;
import com.vortex.api.api.graph.VertexAPI;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.query.QueryResults;
import com.vortex.api.core.GraphManager;
import com.vortex.api.server.RestServer;
import com.vortex.vortexdb.traversal.algorithm.CollectionPathsTraverser;
import com.vortex.vortexdb.traversal.algorithm.VortexTraverser;
import com.vortex.vortexdb.traversal.algorithm.PathsTraverser;
import com.vortex.vortexdb.traversal.algorithm.steps.EdgeStep;
import com.vortex.vortexdb.type.define.Directions;
import com.vortex.common.util.E;
import com.vortex.common.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static com.vortex.vortexdb.traversal.algorithm.VortexTraverser.*;

@Path("graphs/{graph}/traversers/paths")
@Singleton
public class PathsAPI extends TraverserAPI {

    private static final Logger LOG = Log.logger(RestServer.class);

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @QueryParam("source") String source,
                      @QueryParam("target") String target,
                      @QueryParam("direction") String direction,
                      @QueryParam("label") String edgeLabel,
                      @QueryParam("max_depth") int depth,
                      @QueryParam("max_degree")
                      @DefaultValue(DEFAULT_MAX_DEGREE) long maxDegree,
                      @QueryParam("capacity")
                      @DefaultValue(DEFAULT_CAPACITY) long capacity,
                      @QueryParam("limit")
                      @DefaultValue(DEFAULT_PATHS_LIMIT) long limit) {
        LOG.debug("Graph [{}] get paths from '{}', to '{}' with " +
                  "direction {}, edge label {}, max depth '{}', " +
                  "max degree '{}', capacity '{}' and limit '{}'",
                  graph, source, target, direction, edgeLabel, depth,
                  maxDegree, capacity, limit);

        Id sourceId = VertexAPI.checkAndParseVertexId(source);
        Id targetId = VertexAPI.checkAndParseVertexId(target);
        Directions dir = Directions.convert(EdgeAPI.parseDirection(direction));

        Vortex g = graph(manager, graph);
        PathsTraverser traverser = new PathsTraverser(g);
        VortexTraverser.PathSet paths = traverser.paths(sourceId, dir, targetId,
                                                      dir.opposite(), edgeLabel,
                                                      depth, maxDegree, capacity,
                                                      limit);
        return manager.serializer(g).writePaths("paths", paths, false);
    }

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
        E.checkArgumentNotNull(request.step,
                               "The step of request can't be null");
        E.checkArgument(request.depth > 0 && request.depth <= DEFAULT_MAX_DEPTH,
                        "The depth of request must be in (0, %s], " +
                        "but got: %s", DEFAULT_MAX_DEPTH, request.depth);

        LOG.debug("Graph [{}] get paths from source vertices '{}', target " +
                  "vertices '{}', with step '{}', max depth '{}', " +
                  "capacity '{}', limit '{}' and with_vertex '{}'",
                  graph, request.sources, request.targets, request.step,
                  request.depth, request.capacity, request.limit,
                  request.withVertex);

        Vortex g = graph(manager, graph);
        Iterator<Vertex> sources = request.sources.vertices(g);
        Iterator<Vertex> targets = request.targets.vertices(g);
        EdgeStep step = step(g, request.step);

        CollectionPathsTraverser traverser = new CollectionPathsTraverser(g);
        Collection<VortexTraverser.Path> paths;
        paths = traverser.paths(sources, targets, step, request.depth,
                                request.nearest, request.capacity,
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

    private static class Request {

        @JsonProperty("sources")
        public Vertices sources;
        @JsonProperty("targets")
        public Vertices targets;
        @JsonProperty("step")
        public TraverserAPI.Step step;
        @JsonProperty("max_depth")
        public int depth;
        @JsonProperty("nearest")
        public boolean nearest = false;
        @JsonProperty("capacity")
        public long capacity = Long.parseLong(DEFAULT_CAPACITY);
        @JsonProperty("limit")
        public long limit = Long.parseLong(DEFAULT_PATHS_LIMIT);
        @JsonProperty("with_vertex")
        public boolean withVertex = false;

        @Override
        public String toString() {
            return String.format("PathRequest{sources=%s,targets=%s,step=%s," +
                                 "maxDepth=%s,nearest=%s,capacity=%s," +
                                 "limit=%s,withVertex=%s}", this.sources,
                                 this.targets, this.step, this.depth,
                                 this.nearest, this.capacity,
                                 this.limit, this.withVertex);
        }
    }
}
