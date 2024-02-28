
package com.vortex.api.api.auth;

import com.vortex.vortexdb.Vortex;
import com.vortex.api.api.API;
import com.vortex.api.api.filter.AuthenticationFilter;
import com.vortex.api.api.filter.StatusFilter;
import com.vortex.api.api.filter.StatusFilter.Status;
import com.vortex.vortexdb.auth.AuthConstant;
import com.vortex.vortexdb.auth.UserWithRole;
import com.vortex.api.core.GraphManager;
import com.vortex.api.define.Checkable;
import com.vortex.api.server.RestServer;
import com.vortex.common.util.E;
import com.vortex.common.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import javax.inject.Singleton;
import javax.security.sasl.AuthenticationException;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;

@Path("graphs/{graph}/auth")
@Singleton
public class LoginAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Path("login")
    @Status(StatusFilter.Status.OK)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String login(@Context GraphManager manager,
                        @PathParam("graph") String graph,
                        JsonLogin jsonLogin) {
        LOG.debug("Graph [{}] user login: {}", graph, jsonLogin);
        checkCreatingBody(jsonLogin);

        try {
            String token = manager.authManager()
                                  .loginUser(jsonLogin.name, jsonLogin.password);
            Vortex g = graph(manager, graph);
            return manager.serializer(g)
                          .writeMap(ImmutableMap.of("token", token));
        } catch (AuthenticationException e) {
            throw new NotAuthorizedException(e.getMessage(), e);
        }
    }

    @DELETE
    @Timed
    @Path("logout")
    @Status(StatusFilter.Status.OK)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public void logout(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @HeaderParam(HttpHeaders.AUTHORIZATION) String auth) {
        E.checkArgument(StringUtils.isNotEmpty(auth),
                        "Request header Authorization must not be null");
        LOG.debug("Graph [{}] user logout: {}", graph, auth);

        if (!auth.startsWith(AuthenticationFilter.BEARER_TOKEN_PREFIX)) {
            throw new BadRequestException(
                  "Only HTTP Bearer authentication is supported");
        }

        String token = auth.substring(AuthenticationFilter.BEARER_TOKEN_PREFIX
                                                          .length());

        manager.authManager().logoutUser(token);
    }

    @GET
    @Timed
    @Path("verify")
    @Status(StatusFilter.Status.OK)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String verifyToken(@Context GraphManager manager,
                              @PathParam("graph") String graph,
                              @HeaderParam(HttpHeaders.AUTHORIZATION)
                              String token) {
        E.checkArgument(StringUtils.isNotEmpty(token),
                        "Request header Authorization must not be null");
        LOG.debug("Graph [{}] get user: {}", graph, token);

        if (!token.startsWith(AuthenticationFilter.BEARER_TOKEN_PREFIX)) {
            throw new BadRequestException(
                      "Only HTTP Bearer authentication is supported");
        }

        token = token.substring(AuthenticationFilter.BEARER_TOKEN_PREFIX
                                                    .length());
        UserWithRole userWithRole = manager.authManager().validateUser(token);

        Vortex g = graph(manager, graph);
        return manager.serializer(g)
                      .writeMap(ImmutableMap.of(AuthConstant.TOKEN_USER_NAME,
                                                userWithRole.username(),
                                                AuthConstant.TOKEN_USER_ID,
                                                userWithRole.userId()));
    }

    private static class JsonLogin implements Checkable {

        @JsonProperty("user_name")
        private String name;
        @JsonProperty("user_password")
        private String password;

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgument(!StringUtils.isEmpty(this.name),
                            "The name of user can't be null");
            E.checkArgument(!StringUtils.isEmpty(this.password),
                            "The password of user can't be null");
        }

        @Override
        public void checkUpdate() {
            // pass
        }
    }
}
