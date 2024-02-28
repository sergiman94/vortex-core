
package com.vortex.api.api.traversers;

import com.vortex.vortexdb.Vortex;
import com.vortex.api.api.API;
import com.vortex.api.api.graph.EdgeAPI;
import com.vortex.api.api.graph.VertexAPI;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.api.core.GraphManager;
import com.vortex.api.server.RestServer;
import com.vortex.vortexdb.traversal.algorithm.VortexTraverser;
import com.vortex.vortexdb.traversal.algorithm.SubGraphTraverser;
import com.vortex.vortexdb.type.define.Directions;
import com.vortex.common.util.Log;
import com.codahale.metrics.annotation.Timed;
import org.slf4j.Logger;

import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;

import static com.vortex.vortexdb.traversal.algorithm.VortexTraverser.*;

@Path("graphs/{graph}/traversers/rays")
@Singleton
public class RaysAPI extends API {

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
                      @QueryParam("capacity")
                      @DefaultValue(DEFAULT_CAPACITY) long capacity,
                      @QueryParam("limit")
                      @DefaultValue(DEFAULT_PATHS_LIMIT) long limit) {
        LOG.debug("Graph [{}] get rays paths from '{}' with " +
                  "direction '{}', edge label '{}', max depth '{}', " +
                  "max degree '{}', capacity '{}' and limit '{}'",
                  graph, sourceV, direction, edgeLabel, depth, maxDegree,
                  capacity, limit);

        Id source = VertexAPI.checkAndParseVertexId(sourceV);
        Directions dir = Directions.convert(EdgeAPI.parseDirection(direction));

        Vortex g = graph(manager, graph);

        SubGraphTraverser traverser = new SubGraphTraverser(g);
        VortexTraverser.PathSet paths = traverser.rays(source, dir, edgeLabel,
                                                     depth, maxDegree,
                                                     capacity, limit);
        return manager.serializer(g).writePaths("rays", paths, false);
    }
}
