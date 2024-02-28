
package com.vortex.api.api.job;

import com.vortex.vortexdb.Vortex;
import com.vortex.api.api.API;
import com.vortex.api.api.filter.StatusFilter.Status;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.api.core.GraphManager;
import com.vortex.common.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;

import javax.annotation.security.RolesAllowed;
import javax.inject.Singleton;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import java.util.Map;

@Path("graphs/{graph}/jobs/rebuild")
@Singleton
public class RebuildAPI extends API {

    private static final Logger LOG = Log.logger(RebuildAPI.class);

    @PUT
    @Timed
    @Path("vertexlabels/{name}")
    @Status(Status.ACCEPTED)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$graph $action=index_write"})
    public Map<String, Id> vertexLabelRebuild(@Context GraphManager manager,
                                              @PathParam("graph") String graph,
                                              @PathParam("name") String name) {
        LOG.debug("Graph [{}] rebuild vertex label: {}", graph, name);

        Vortex g = graph(manager, graph);
        return ImmutableMap.of("task_id",
                               g.schema().vertexLabel(name).rebuildIndex());
    }

    @PUT
    @Timed
    @Path("edgelabels/{name}")
    @Status(Status.ACCEPTED)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$graph $action=index_write"})
    public Map<String, Id> edgeLabelRebuild(@Context GraphManager manager,
                                            @PathParam("graph") String graph,
                                            @PathParam("name") String name) {
        LOG.debug("Graph [{}] rebuild edge label: {}", graph, name);

        Vortex g = graph(manager, graph);
        return ImmutableMap.of("task_id",
                               g.schema().edgeLabel(name).rebuildIndex());
    }

    @PUT
    @Timed
    @Path("indexlabels/{name}")
    @Status(Status.ACCEPTED)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$graph $action=index_write"})
    public Map<String, Id> indexLabelRebuild(@Context GraphManager manager,
                                             @PathParam("graph") String graph,
                                             @PathParam("name") String name) {
        LOG.debug("Graph [{}] rebuild index label: {}", graph, name);

        Vortex g = graph(manager, graph);
        return ImmutableMap.of("task_id",
                               g.schema().indexLabel(name).rebuild());
    }
}