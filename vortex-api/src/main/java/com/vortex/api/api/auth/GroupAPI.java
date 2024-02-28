
package com.vortex.api.api.auth;

import com.vortex.vortexdb.Vortex;
import com.vortex.api.api.API;
import com.vortex.api.api.filter.StatusFilter.Status;
import com.vortex.vortexdb.auth.VortexGroup;
import com.vortex.vortexdb.backend.id.IdGenerator;
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

@Path("graphs/{graph}/auth/groups")
@Singleton
public class GroupAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String create(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         JsonGroup jsonGroup) {
        LOG.debug("Graph [{}] create group: {}", graph, jsonGroup);
        checkCreatingBody(jsonGroup);

        Vortex g = graph(manager, graph);
        VortexGroup group = jsonGroup.build();
        group.id(manager.authManager().createGroup(group));
        return manager.serializer(g).writeAuthElement(group);
    }

    @PUT
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String update(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         @PathParam("id") String id,
                         JsonGroup jsonGroup) {
        LOG.debug("Graph [{}] update group: {}", graph, jsonGroup);
        checkUpdatingBody(jsonGroup);

        Vortex g = graph(manager, graph);
        VortexGroup group;
        try {
            group = manager.authManager().getGroup(UserAPI.parseId(id));
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid group id: " + id);
        }
        group = jsonGroup.build(group);
        manager.authManager().updateGroup(group);
        return manager.serializer(g).writeAuthElement(group);
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String list(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @QueryParam("limit") @DefaultValue("100") long limit) {
        LOG.debug("Graph [{}] list groups", graph);

        Vortex g = graph(manager, graph);
        List<VortexGroup> groups = manager.authManager().listAllGroups(limit);
        return manager.serializer(g).writeAuthElements("groups", groups);
    }

    @GET
    @Timed
    @Path("{id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @PathParam("id") String id) {
        LOG.debug("Graph [{}] get group: {}", graph, id);

        Vortex g = graph(manager, graph);
        VortexGroup group = manager.authManager().getGroup(IdGenerator.of(id));
        return manager.serializer(g).writeAuthElement(group);
    }

    @DELETE
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @PathParam("id") String id) {
        LOG.debug("Graph [{}] delete group: {}", graph, id);

        @SuppressWarnings("unused") // just check if the graph exists
        Vortex g = graph(manager, graph);
        try {
            manager.authManager().deleteGroup(IdGenerator.of(id));
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid group id: " + id);
        }
    }

    @JsonIgnoreProperties(value = {"id", "group_creator",
                                   "group_create", "group_update"})
    private static class JsonGroup implements Checkable {

        @JsonProperty("group_name")
        private String name;
        @JsonProperty("group_description")
        private String description;

        public VortexGroup build(VortexGroup group) {
            E.checkArgument(this.name == null || group.name().equals(this.name),
                            "The name of group can't be updated");
            if (this.description != null) {
                group.description(this.description);
            }
            return group;
        }

        public VortexGroup build() {
            VortexGroup group = new VortexGroup(this.name);
            group.description(this.description);
            return group;
        }

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgumentNotNull(this.name,
                                   "The name of group can't be null");
        }

        @Override
        public void checkUpdate() {
            E.checkArgumentNotNull(this.description,
                                   "The description of group can't be null");
        }
    }
}
