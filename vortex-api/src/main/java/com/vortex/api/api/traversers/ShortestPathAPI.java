
package com.vortex.api.api.traversers;

import com.vortex.vortexdb.Vortex;
import com.vortex.api.api.API;
import com.vortex.api.api.graph.EdgeAPI;
import com.vortex.api.api.graph.VertexAPI;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.api.core.GraphManager;
import com.vortex.api.server.RestServer;
import com.vortex.vortexdb.traversal.algorithm.VortexTraverser;
import com.vortex.vortexdb.traversal.algorithm.ShortestPathTraverser;
import com.vortex.vortexdb.type.define.Directions;
import com.vortex.common.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;

import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import java.util.List;

import static com.vortex.vortexdb.traversal.algorithm.VortexTraverser.DEFAULT_CAPACITY;
import static com.vortex.vortexdb.traversal.algorithm.VortexTraverser.DEFAULT_MAX_DEGREE;

@Path("graphs/{graph}/traversers/shortestpath")
@Singleton
public class ShortestPathAPI extends API {

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
                      @QueryParam("skip_degree")
                      @DefaultValue("0") long skipDegree,
                      @QueryParam("capacity")
                      @DefaultValue(DEFAULT_CAPACITY) long capacity) {
        LOG.debug("Graph [{}] get shortest path from '{}', to '{}' with " +
                  "direction {}, edge label {}, max depth '{}', " +
                  "max degree '{}', skipped maxDegree '{}' and capacity '{}'",
                  graph, source, target, direction, edgeLabel, depth,
                  maxDegree, skipDegree, capacity);
        Id sourceId = VertexAPI.checkAndParseVertexId(source);
        Id targetId = VertexAPI.checkAndParseVertexId(target);
        Directions dir = Directions.convert(EdgeAPI.parseDirection(direction));

        Vortex g = graph(manager, graph);

        ShortestPathTraverser traverser = new ShortestPathTraverser(g);

        List<String> edgeLabels = edgeLabel == null ? ImmutableList.of() :
                                  ImmutableList.of(edgeLabel);
        VortexTraverser.Path path = traverser.shortestPath(sourceId, targetId,
                                                         dir, edgeLabels, depth,
                                                         maxDegree, skipDegree,
                                                         capacity);
        return manager.serializer(g).writeList("path", path.vertices());
    }
}
