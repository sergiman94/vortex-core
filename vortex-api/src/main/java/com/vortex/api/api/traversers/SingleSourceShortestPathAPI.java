
package com.vortex.api.api.traversers;

import com.vortex.vortexdb.Vortex;
import com.vortex.api.api.API;
import com.vortex.api.api.graph.EdgeAPI;
import com.vortex.api.api.graph.VertexAPI;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.query.QueryResults;
import com.vortex.api.core.GraphManager;
import com.vortex.api.server.RestServer;
import com.vortex.vortexdb.traversal.algorithm.SingleSourceShortestPathTraverser;
import com.vortex.vortexdb.traversal.algorithm.SingleSourceShortestPathTraverser.WeightedPaths;
import com.vortex.vortexdb.type.define.Directions;
import com.vortex.common.util.Log;
import com.codahale.metrics.annotation.Timed;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import java.util.Iterator;

import static com.vortex.vortexdb.traversal.algorithm.VortexTraverser.*;

@Path("graphs/{graph}/traversers/singlesourceshortestpath")
@Singleton
public class SingleSourceShortestPathAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @QueryParam("source") String source,
                      @QueryParam("direction") String direction,
                      @QueryParam("label") String edgeLabel,
                      @QueryParam("weight") String weight,
                      @QueryParam("max_degree")
                      @DefaultValue(DEFAULT_MAX_DEGREE) long maxDegree,
                      @QueryParam("skip_degree")
                      @DefaultValue("0") long skipDegree,
                      @QueryParam("capacity")
                      @DefaultValue(DEFAULT_CAPACITY) long capacity,
                      @QueryParam("limit")
                      @DefaultValue(DEFAULT_PATHS_LIMIT) long limit,
                      @QueryParam("with_vertex") boolean withVertex) {
        LOG.debug("Graph [{}] get single source shortest path from '{}' " +
                  "with direction {}, edge label {}, weight property {}, " +
                  "max degree '{}', limit '{}' and with vertex '{}'",
                  graph, source, direction, edgeLabel,
                  weight, maxDegree, withVertex);

        Id sourceId = VertexAPI.checkAndParseVertexId(source);
        Directions dir = Directions.convert(EdgeAPI.parseDirection(direction));

        Vortex g = graph(manager, graph);
        SingleSourceShortestPathTraverser traverser =
                new SingleSourceShortestPathTraverser(g);
        WeightedPaths paths = traverser.singleSourceShortestPaths(
                              sourceId, dir, edgeLabel, weight,
                              maxDegree, skipDegree, capacity, limit);
        Iterator<Vertex> iterator = QueryResults.emptyIterator();
        assert paths != null;
        if (!paths.isEmpty() && withVertex) {
            iterator = g.vertices(paths.vertices().toArray());
        }
        return manager.serializer(g).writeWeightedPaths(paths, iterator);
    }
}
