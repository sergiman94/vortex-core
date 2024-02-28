
package com.vortex.api.api.traversers;

import com.vortex.vortexdb.Vortex;
import com.vortex.api.api.API;
import com.vortex.api.api.graph.EdgeAPI;
import com.vortex.api.api.graph.VertexAPI;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.api.core.GraphManager;
import com.vortex.api.server.RestServer;
import com.vortex.vortexdb.traversal.algorithm.SameNeighborTraverser;
import com.vortex.vortexdb.type.define.Directions;
import com.vortex.common.util.Log;
import com.codahale.metrics.annotation.Timed;
import org.slf4j.Logger;

import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import java.util.Set;

import static com.vortex.vortexdb.traversal.algorithm.VortexTraverser.DEFAULT_ELEMENTS_LIMIT;
import static com.vortex.vortexdb.traversal.algorithm.VortexTraverser.DEFAULT_MAX_DEGREE;

@Path("graphs/{graph}/traversers/sameneighbors")
@Singleton
public class SameNeighborsAPI extends API {

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
                      @DefaultValue(DEFAULT_MAX_DEGREE) long maxDegree,
                      @QueryParam("limit")
                      @DefaultValue(DEFAULT_ELEMENTS_LIMIT) long limit) {
        LOG.debug("Graph [{}] get same neighbors between '{}' and '{}' with " +
                  "direction {}, edge label {}, max degree '{}' and limit '{}'",
                  graph, vertex, other, direction, edgeLabel, maxDegree, limit);

        Id sourceId = VertexAPI.checkAndParseVertexId(vertex);
        Id targetId = VertexAPI.checkAndParseVertexId(other);
        Directions dir = Directions.convert(EdgeAPI.parseDirection(direction));

        Vortex g = graph(manager, graph);
        SameNeighborTraverser traverser = new SameNeighborTraverser(g);
        Set<Id> neighbors = traverser.sameNeighbors(sourceId, targetId, dir,
                                                    edgeLabel, maxDegree, limit);
        return manager.serializer(g).writeList("same_neighbors", neighbors);
    }
}
