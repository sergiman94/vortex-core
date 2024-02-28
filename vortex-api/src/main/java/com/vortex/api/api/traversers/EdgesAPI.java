
package com.vortex.api.api.traversers;

import com.vortex.vortexdb.Vortex;
import com.vortex.api.api.API;
import com.vortex.api.api.filter.CompressInterceptor.Compress;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.query.ConditionQuery;
import com.vortex.vortexdb.backend.store.Shard;
import com.vortex.api.core.GraphManager;
import com.vortex.api.server.RestServer;
import com.vortex.vortexdb.structure.VortexEdge;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.common.util.E;
import com.vortex.common.util.Log;
import com.codahale.metrics.annotation.Timed;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.slf4j.Logger;

import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import java.util.Iterator;
import java.util.List;

import static com.vortex.vortexdb.traversal.algorithm.VortexTraverser.DEFAULT_PAGE_LIMIT;

@Path("graphs/{graph}/traversers/edges")
@Singleton
public class EdgesAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @GET
    @Timed
    @Compress
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String list(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @QueryParam("ids") List<String> stringIds) {
        LOG.debug("Graph [{}] get edges by ids: {}", graph, stringIds);

        E.checkArgument(stringIds != null && !stringIds.isEmpty(),
                        "The ids parameter can't be null or empty");

        Object[] ids = new Id[stringIds.size()];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = VortexEdge.getIdValue(stringIds.get(i), false);
        }

        Vortex g = graph(manager, graph);

        Iterator<Edge> edges = g.edges(ids);
        return manager.serializer(g).writeEdges(edges, false);
    }

    @GET
    @Timed
    @Path("shards")
    @Compress
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String shards(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         @QueryParam("split_size") long splitSize) {
        LOG.debug("Graph [{}] get vertex shards with split size '{}'",
                  graph, splitSize);

        Vortex g = graph(manager, graph);
        List<Shard> shards = g.metadata(VortexType.EDGE_OUT, "splits", splitSize);
        return manager.serializer(g).writeList("shards", shards);
    }

    @GET
    @Timed
    @Path("scan")
    @Compress
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String scan(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @QueryParam("start") String start,
                       @QueryParam("end") String end,
                       @QueryParam("page") String page,
                       @QueryParam("page_limit")
                       @DefaultValue(DEFAULT_PAGE_LIMIT) long pageLimit) {
        LOG.debug("Graph [{}] query edges by shard(start: {}, end: {}, " +
                  "page: {}) ", graph, start, end, page);

        Vortex g = graph(manager, graph);

        ConditionQuery query = new ConditionQuery(VortexType.EDGE_OUT);
        query.scan(start, end);
        query.page(page);
        if (query.paging()) {
            query.limit(pageLimit);
        }
        Iterator<Edge> edges = g.edges(query);

        return manager.serializer(g).writeEdges(edges, query.paging());
    }
}
