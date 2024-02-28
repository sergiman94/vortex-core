
package com.vortex.api.api.auth;

import com.vortex.vortexdb.Vortex;
import com.vortex.api.api.API;
import com.vortex.api.api.filter.StatusFilter.Status;
import com.vortex.vortexdb.auth.VortexBelong;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.api.core.GraphManager;
import com.vortex.api.define.Checkable;
import com.vortex.vortexdb.exception.NotFoundException;
import com.vortex.api.server.RestServer;
import com.vortex.common.util.E;
import com.vortex.common.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;

import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import java.util.List;

@Path("graphs/{graph}/auth/belongs")
@Singleton
public class BelongAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String create(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         JsonBelong jsonBelong) {
        LOG.debug("Graph [{}] create belong: {}", graph, jsonBelong);
        checkCreatingBody(jsonBelong);

        Vortex g = graph(manager, graph);
        VortexBelong belong = jsonBelong.build();
        belong.id(manager.authManager().createBelong(belong));
        return manager.serializer(g).writeAuthElement(belong);
    }

    @PUT
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String update(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         @PathParam("id") String id,
                         JsonBelong jsonBelong) {
        LOG.debug("Graph [{}] update belong: {}", graph, jsonBelong);
        checkUpdatingBody(jsonBelong);

        Vortex g = graph(manager, graph);
        VortexBelong belong;
        try {
            belong = manager.authManager().getBelong(UserAPI.parseId(id));
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid belong id: " + id);
        }
        belong = jsonBelong.build(belong);
        manager.authManager().updateBelong(belong);
        return manager.serializer(g).writeAuthElement(belong);
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String list(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @QueryParam("user") String user,
                       @QueryParam("group") String group,
                       @QueryParam("limit") @DefaultValue("100") long limit) {
        LOG.debug("Graph [{}] list belongs by user {} or group {}",
                  graph, user, group);
        E.checkArgument(user == null || group == null,
                        "Can't pass both user and group at the same time");

        Vortex g = graph(manager, graph);
        List<VortexBelong> belongs;
        if (user != null) {
            Id id = UserAPI.parseId(user);
            belongs = manager.authManager().listBelongByUser(id, limit);
        } else if (group != null) {
            Id id = UserAPI.parseId(group);
            belongs = manager.authManager().listBelongByGroup(id, limit);
        } else {
            belongs = manager.authManager().listAllBelong(limit);
        }
        return manager.serializer(g).writeAuthElements("belongs", belongs);
    }

    @GET
    @Timed
    @Path("{id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @PathParam("id") String id) {
        LOG.debug("Graph [{}] get belong: {}", graph, id);

        Vortex g = graph(manager, graph);
        VortexBelong belong = manager.authManager().getBelong(UserAPI.parseId(id));
        return manager.serializer(g).writeAuthElement(belong);
    }

    @DELETE
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @PathParam("id") String id) {
        LOG.debug("Graph [{}] delete belong: {}", graph, id);

        @SuppressWarnings("unused") // just check if the graph exists
        Vortex g = graph(manager, graph);
        try {
            manager.authManager().deleteBelong(UserAPI.parseId(id));
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid belong id: " + id);
        }
    }

    @JsonIgnoreProperties(value = {"id", "belong_creator",
                                   "belong_create", "belong_update"})
    private static class JsonBelong implements Checkable {

        @JsonProperty("user")
        private String user;
        @JsonProperty("group")
        private String group;
        @JsonProperty("belong_description")
        private String description;

        public VortexBelong build(VortexBelong belong) {
            E.checkArgument(this.user == null ||
                            belong.source().equals(UserAPI.parseId(this.user)),
                            "The user of belong can't be updated");
            E.checkArgument(this.group == null ||
                            belong.target().equals(UserAPI.parseId(this.group)),
                            "The group of belong can't be updated");
            if (this.description != null) {
                belong.description(this.description);
            }
            return belong;
        }

        public VortexBelong build() {
            VortexBelong belong = new VortexBelong(UserAPI.parseId(this.user),
                                               UserAPI.parseId(this.group));
            belong.description(this.description);
            return belong;
        }

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgumentNotNull(this.user,
                                   "The user of belong can't be null");
            E.checkArgumentNotNull(this.group,
                                   "The group of belong can't be null");
        }

        @Override
        public void checkUpdate() {
            E.checkArgumentNotNull(this.description,
                                   "The description of belong can't be null");
        }
    }
}
