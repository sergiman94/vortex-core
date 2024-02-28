
package com.vortex.api.api.raft;

import com.vortex.vortexdb.VortexException;
import com.vortex.vortexdb.Vortex;
import com.vortex.api.api.API;
import com.vortex.api.api.filter.StatusFilter.Status;
import com.vortex.vortexdb.backend.store.raft.RaftGroupManager;
import com.vortex.api.core.GraphManager;
import com.vortex.common.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;

import javax.annotation.security.RolesAllowed;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import java.util.List;
import java.util.Map;

@Path("graphs/{graph}/raft")
@Singleton
public class RaftAPI extends API {

    private static final Logger LOG = Log.logger(RaftAPI.class);

    @GET
    @Timed
    @Path("list_peers")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public Map<String, List<String>> listPeers(@Context GraphManager manager,
                                               @PathParam("graph") String graph,
                                               @QueryParam("group")
                                               @DefaultValue("default")
                                               String group) {
        LOG.debug("Graph [{}] prepare to get leader", graph);

        Vortex g = graph(manager, graph);
        RaftGroupManager raftManager = raftGroupManager(g, group, "list_peers");
        List<String> peers = raftManager.listPeers();
        return ImmutableMap.of(raftManager.group(), peers);
    }

    @GET
    @Timed
    @Path("get_leader")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public Map<String, String> getLeader(@Context GraphManager manager,
                                         @PathParam("graph") String graph,
                                         @QueryParam("group")
                                         @DefaultValue("default")
                                         String group) {
        LOG.debug("Graph [{}] prepare to get leader", graph);

        Vortex g = graph(manager, graph);
        RaftGroupManager raftManager = raftGroupManager(g, group, "get_leader");
        String leaderId = raftManager.getLeader();
        return ImmutableMap.of(raftManager.group(), leaderId);
    }

    @POST
    @Timed
    @Status(Status.OK)
    @Path("transfer_leader")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public Map<String, String> transferLeader(@Context GraphManager manager,
                                              @PathParam("graph") String graph,
                                              @QueryParam("group")
                                              @DefaultValue("default")
                                              String group,
                                              @QueryParam("endpoint")
                                              String endpoint) {
        LOG.debug("Graph [{}] prepare to transfer leader to: {}",
                  graph, endpoint);

        Vortex g = graph(manager, graph);
        RaftGroupManager raftManager = raftGroupManager(g, group,
                                                        "transfer_leader");
        String leaderId = raftManager.transferLeaderTo(endpoint);
        return ImmutableMap.of(raftManager.group(), leaderId);
    }

    @POST
    @Timed
    @Status(Status.OK)
    @Path("set_leader")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public Map<String, String> setLeader(@Context GraphManager manager,
                                         @PathParam("graph") String graph,
                                         @QueryParam("group")
                                         @DefaultValue("default")
                                         String group,
                                         @QueryParam("endpoint")
                                         String endpoint) {
        LOG.debug("Graph [{}] prepare to set leader to: {}",
                  graph, endpoint);

        Vortex g = graph(manager, graph);
        RaftGroupManager raftManager = raftGroupManager(g, group, "set_leader");
        String leaderId = raftManager.setLeader(endpoint);
        return ImmutableMap.of(raftManager.group(), leaderId);
    }

    @POST
    @Timed
    @Status(Status.OK)
    @Path("add_peer")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public Map<String, String> addPeer(@Context GraphManager manager,
                                       @PathParam("graph") String graph,
                                       @QueryParam("group")
                                       @DefaultValue("default")
                                       String group,
                                       @QueryParam("endpoint")
                                       String endpoint) {
        LOG.debug("Graph [{}] prepare to add peer: {}", graph, endpoint);

        Vortex g = graph(manager, graph);
        RaftGroupManager raftManager = raftGroupManager(g, group, "add_peer");
        String peerId = raftManager.addPeer(endpoint);
        return ImmutableMap.of(raftManager.group(), peerId);
    }

    @POST
    @Timed
    @Status(Status.OK)
    @Path("remove_peer")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public Map<String, String> removePeer(@Context GraphManager manager,
                                          @PathParam("graph") String graph,
                                          @QueryParam("group")
                                          @DefaultValue("default")
                                          String group,
                                          @QueryParam("endpoint")
                                          String endpoint) {
        LOG.debug("Graph [{}] prepare to remove peer: {}", graph, endpoint);

        Vortex g = graph(manager, graph);
        RaftGroupManager raftManager = raftGroupManager(g, group,
                                                        "remove_peer");
        String peerId = raftManager.removePeer(endpoint);
        return ImmutableMap.of(raftManager.group(), peerId);
    }

    private static RaftGroupManager raftGroupManager(Vortex graph,
                                                     String group,
                                                     String operation) {
        RaftGroupManager raftManager = graph.raftGroupManager(group);
        if (raftManager == null) {
            throw new VortexException("Allowed %s operation only when " +
                                    "working on raft mode", operation);
        }
        return raftManager;
    }
}
