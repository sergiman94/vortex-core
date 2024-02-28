
package com.vortex.api.api.auth;

import com.vortex.vortexdb.Vortex;
import com.vortex.api.api.API;
import com.vortex.api.api.filter.StatusFilter.Status;
import com.vortex.vortexdb.auth.VortexUser;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.id.IdGenerator;
import com.vortex.api.core.GraphManager;
import com.vortex.api.define.Checkable;
import com.vortex.vortexdb.exception.NotFoundException;
import com.vortex.api.server.RestServer;
import com.vortex.common.util.E;
import com.vortex.common.util.Log;
import com.vortex.vortexdb.util.StringEncoding;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import java.util.List;

@Path("graphs/{graph}/auth/users")
@Singleton
public class UserAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String create(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         JsonUser jsonUser) {
        LOG.debug("Graph [{}] create user: {}", graph, jsonUser);
        checkCreatingBody(jsonUser);

        Vortex g = graph(manager, graph);
        VortexUser user = jsonUser.build();
        user.id(manager.authManager().createUser(user));
        return manager.serializer(g).writeAuthElement(user);
    }

    @PUT
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String update(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         @PathParam("id") String id,
                         JsonUser jsonUser) {
        LOG.debug("Graph [{}] update user: {}", graph, jsonUser);
        checkUpdatingBody(jsonUser);

        Vortex g = graph(manager, graph);
        VortexUser user;
        try {
            user = manager.authManager().getUser(UserAPI.parseId(id));
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid user id: " + id);
        }
        user = jsonUser.build(user);
        manager.authManager().updateUser(user);
        return manager.serializer(g).writeAuthElement(user);
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String list(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @QueryParam("limit") @DefaultValue("100") long limit) {
        LOG.debug("Graph [{}] list users", graph);

        Vortex g = graph(manager, graph);
        List<VortexUser> users = manager.authManager().listAllUsers(limit);
        return manager.serializer(g).writeAuthElements("users", users);
    }

    @GET
    @Timed
    @Path("{id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @PathParam("id") String id) {
        LOG.debug("Graph [{}] get user: {}", graph, id);

        Vortex g = graph(manager, graph);
        VortexUser user = manager.authManager().getUser(IdGenerator.of(id));
        return manager.serializer(g).writeAuthElement(user);
    }

    @GET
    @Timed
    @Path("{id}/role")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String role(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @PathParam("id") String id) {
        LOG.debug("Graph [{}] get user role: {}", graph, id);

        @SuppressWarnings("unused") // just check if the graph exists
        Vortex g = graph(manager, graph);
        VortexUser user = manager.authManager().getUser(IdGenerator.of(id));
        return manager.authManager().rolePermission(user).toJson();
    }

    @DELETE
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @PathParam("id") String id) {
        LOG.debug("Graph [{}] delete user: {}", graph, id);

        @SuppressWarnings("unused") // just check if the graph exists
        Vortex g = graph(manager, graph);
        try {
            manager.authManager().deleteUser(IdGenerator.of(id));
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid user id: " + id);
        }
    }

    protected static Id parseId(String id) {
        return IdGenerator.of(id);
    }

    @JsonIgnoreProperties(value = {"id", "user_creator",
                                   "user_create", "user_update"})
    private static class JsonUser implements Checkable {

        @JsonProperty("user_name")
        private String name;
        @JsonProperty("user_password")
        private String password;
        @JsonProperty("user_phone")
        private String phone;
        @JsonProperty("user_email")
        private String email;
        @JsonProperty("user_avatar")
        private String avatar;
        @JsonProperty("user_description")
        private String description;

        public VortexUser build(VortexUser user) {
            E.checkArgument(this.name == null || user.name().equals(this.name),
                            "The name of user can't be updated");
            if (this.password != null) {
                user.password(StringEncoding.hashPassword(this.password));
            }
            if (this.phone != null) {
                user.phone(this.phone);
            }
            if (this.email != null) {
                user.email(this.email);
            }
            if (this.avatar != null) {
                user.avatar(this.avatar);
            }
            if (this.description != null) {
                user.description(this.description);
            }
            return user;
        }

        public VortexUser build() {
            VortexUser user = new VortexUser(this.name);
            user.password(StringEncoding.hashPassword(this.password));
            user.phone(this.phone);
            user.email(this.email);
            user.avatar(this.avatar);
            user.description(this.description);
            return user;
        }

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgument(!StringUtils.isEmpty(this.name),
                            "The name of user can't be null");
            E.checkArgument(!StringUtils.isEmpty(this.password),
                            "The password of user can't be null");
        }

        @Override
        public void checkUpdate() {
            E.checkArgument(!StringUtils.isEmpty(this.password) ||
                            this.phone != null ||
                            this.email != null ||
                            this.avatar != null,
                            "Expect one of user password/phone/email/avatar]");
        }
    }
}
