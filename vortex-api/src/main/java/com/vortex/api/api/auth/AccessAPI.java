
package com.vortex.api.api.auth;

import com.vortex.vortexdb.Vortex;
import com.vortex.api.api.API;
import com.vortex.api.api.filter.StatusFilter.Status;
import com.vortex.vortexdb.auth.VortexAccess;
import com.vortex.vortexdb.auth.VortexPermission;
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

@Path("graphs/{graph}/auth/accesses")
@Singleton
public class AccessAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String create(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         JsonAccess jsonAccess) {
        LOG.debug("Graph [{}] create access: {}", graph, jsonAccess);
        checkCreatingBody(jsonAccess);

        Vortex g = graph(manager, graph);
        VortexAccess access = jsonAccess.build();
        access.id(manager.authManager().createAccess(access));
        return manager.serializer(g).writeAuthElement(access);
    }

    @PUT
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String update(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         @PathParam("id") String id,
                         JsonAccess jsonAccess) {
        LOG.debug("Graph [{}] update access: {}", graph, jsonAccess);
        checkUpdatingBody(jsonAccess);

        Vortex g = graph(manager, graph);
        VortexAccess access;
        try {
            access = manager.authManager().getAccess(UserAPI.parseId(id));
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid access id: " + id);
        }
        access = jsonAccess.build(access);
        manager.authManager().updateAccess(access);
        return manager.serializer(g).writeAuthElement(access);
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String list(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @QueryParam("group") String group,
                       @QueryParam("target") String target,
                       @QueryParam("limit") @DefaultValue("100") long limit) {
        LOG.debug("Graph [{}] list belongs by group {} or target {}",
                  graph, group, target);
        E.checkArgument(group == null || target == null,
                        "Can't pass both group and target at the same time");

        Vortex g = graph(manager, graph);
        List<VortexAccess> belongs;
        if (group != null) {
            Id id = UserAPI.parseId(group);
            belongs = manager.authManager().listAccessByGroup(id, limit);
        } else if (target != null) {
            Id id = UserAPI.parseId(target);
            belongs = manager.authManager().listAccessByTarget(id, limit);
        } else {
            belongs = manager.authManager().listAllAccess(limit);
        }
        return manager.serializer(g).writeAuthElements("accesses", belongs);
    }

    @GET
    @Timed
    @Path("{id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @PathParam("id") String id) {
        LOG.debug("Graph [{}] get access: {}", graph, id);

        Vortex g = graph(manager, graph);
        VortexAccess access = manager.authManager().getAccess(UserAPI.parseId(id));
        return manager.serializer(g).writeAuthElement(access);
    }

    @DELETE
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @PathParam("id") String id) {
        LOG.debug("Graph [{}] delete access: {}", graph, id);

        @SuppressWarnings("unused") // just check if the graph exists
        Vortex g = graph(manager, graph);
        try {
            manager.authManager().deleteAccess(UserAPI.parseId(id));
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid access id: " + id);
        }
    }

    @JsonIgnoreProperties(value = {"id", "access_creator",
                                   "access_create", "access_update"})
    private static class JsonAccess implements Checkable {

        @JsonProperty("group")
        private String group;
        @JsonProperty("target")
        private String target;
        @JsonProperty("access_permission")
        private VortexPermission permission;
        @JsonProperty("access_description")
        private String description;

        public VortexAccess build(VortexAccess access) {
            E.checkArgument(this.group == null ||
                            access.source().equals(UserAPI.parseId(this.group)),
                            "The group of access can't be updated");
            E.checkArgument(this.target == null ||
                            access.target().equals(UserAPI.parseId(this.target)),
                            "The target of access can't be updated");
            E.checkArgument(this.permission == null ||
                            access.permission().equals(this.permission),
                            "The permission of access can't be updated");
            if (this.description != null) {
                access.description(this.description);
            }
            return access;
        }

        public VortexAccess build() {
            VortexAccess access = new VortexAccess(UserAPI.parseId(this.group),
                                               UserAPI.parseId(this.target));
            access.permission(this.permission);
            access.description(this.description);
            return access;
        }

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgumentNotNull(this.group,
                                   "The group of access can't be null");
            E.checkArgumentNotNull(this.target,
                                   "The target of access can't be null");
            E.checkArgumentNotNull(this.permission,
                                   "The permission of access can't be null");
        }

        @Override
        public void checkUpdate() {
            E.checkArgumentNotNull(this.description,
                                   "The description of access can't be null");
        }
    }
}
