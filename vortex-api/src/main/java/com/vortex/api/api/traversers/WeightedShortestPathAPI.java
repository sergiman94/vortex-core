
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
import com.vortex.vortexdb.traversal.algorithm.SingleSourceShortestPathTraverser.NodeWithWeight;
import com.vortex.vortexdb.type.define.Directions;
import com.vortex.common.util.E;
import com.vortex.common.util.Log;
import com.codahale.metrics.annotation.Timed;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import java.util.Iterator;

import static com.vortex.vortexdb.traversal.algorithm.VortexTraverser.DEFAULT_CAPACITY;
import static com.vortex.vortexdb.traversal.algorithm.VortexTraverser.DEFAULT_MAX_DEGREE;

@Path("graphs/{graph}/traversers/weightedshortestpath")
@Singleton
public class WeightedShortestPathAPI extends API {

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
                      @QueryParam("weight") String weight,
                      @QueryParam("max_degree")
                      @DefaultValue(DEFAULT_MAX_DEGREE) long maxDegree,
                      @QueryParam("skip_degree")
                      @DefaultValue("0") long skipDegree,
                      @QueryParam("capacity")
                      @DefaultValue(DEFAULT_CAPACITY) long capacity,
                      @QueryParam("with_vertex") boolean withVertex) {
        LOG.debug("Graph [{}] get weighted shortest path between '{}' and " +
                  "'{}' with direction {}, edge label {}, weight property {}, " +
                  "max degree '{}', skip degree '{}', capacity '{}', " +
                  "and with vertex '{}'",
                  graph, source, target, direction, edgeLabel, weight,
                  maxDegree, skipDegree, capacity, withVertex);

        Id sourceId = VertexAPI.checkAndParseVertexId(source);
        Id targetId = VertexAPI.checkAndParseVertexId(target);
        Directions dir = Directions.convert(EdgeAPI.parseDirection(direction));
        E.checkArgumentNotNull(weight, "The weight property can't be null");

        Vortex g = graph(manager, graph);
        SingleSourceShortestPathTraverser traverser =
                new SingleSourceShortestPathTraverser(g);

        NodeWithWeight path = traverser.weightedShortestPath(
                              sourceId, targetId, dir, edgeLabel, weight,
                              maxDegree, skipDegree, capacity);
        Iterator<Vertex> iterator = QueryResults.emptyIterator();
        if (path != null && withVertex) {
            assert !path.node().path().isEmpty();
            iterator = g.vertices(path.node().path().toArray());
        }
        return manager.serializer(g).writeWeightedPath(path, iterator);
    }
}
