
package com.vortex.api.api.traversers;

import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.query.QueryResults;
import com.vortex.api.core.GraphManager;
import com.vortex.api.server.RestServer;
import com.vortex.vortexdb.traversal.algorithm.VortexTraverser;
import com.vortex.vortexdb.traversal.algorithm.MultiNodeShortestPathTraverser;
import com.vortex.vortexdb.traversal.algorithm.steps.EdgeStep;
import com.vortex.common.util.E;
import com.vortex.common.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.vortex.vortexdb.traversal.algorithm.VortexTraverser.DEFAULT_CAPACITY;

@Path("graphs/{graph}/traversers/multinodeshortestpath")
@Singleton
public class MultiNodeShortestPathAPI extends TraverserAPI {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String post(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       Request request) {
        E.checkArgumentNotNull(request, "The request body can't be null");
        E.checkArgumentNotNull(request.vertices,
                               "The vertices of request can't be null");
        E.checkArgument(request.step != null,
                        "The steps of request can't be null");

        LOG.debug("Graph [{}] get multiple node shortest path from " +
                  "vertices '{}', with step '{}', max_depth '{}', capacity " +
                  "'{}' and with_vertex '{}'",
                  graph, request.vertices, request.step, request.maxDepth,
                  request.capacity, request.withVertex);

        Vortex g = graph(manager, graph);
        Iterator<Vertex> vertices = request.vertices.vertices(g);

        EdgeStep step = step(g, request.step);

        List<VortexTraverser.Path> paths;
        try (MultiNodeShortestPathTraverser traverser =
                                        new MultiNodeShortestPathTraverser(g)) {
            paths = traverser.multiNodeShortestPath(vertices, step,
                                                    request.maxDepth,
                                                    request.capacity);
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

    private static class Request {

        @JsonProperty("vertices")
        public Vertices vertices;
        @JsonProperty("step")
        public Step step;
        @JsonProperty("max_depth")
        public int maxDepth;
        @JsonProperty("capacity")
        public long capacity = Long.parseLong(DEFAULT_CAPACITY);
        @JsonProperty("with_vertex")
        public boolean withVertex = false;

        @Override
        public String toString() {
            return String.format("Request{vertices=%s,step=%s,maxDepth=%s" +
                                 "capacity=%s,withVertex=%s}",
                                 this.vertices, this.step, this.maxDepth,
                                 this.capacity, this.withVertex);
        }
    }
}
