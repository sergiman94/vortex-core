
package com.vortex.api.api.auth;

import com.vortex.vortexdb.Vortex;
import com.vortex.api.api.API;
import com.vortex.api.api.filter.StatusFilter.Status;
import com.vortex.vortexdb.auth.VortexTarget;
import com.vortex.api.core.GraphManager;
import com.vortex.api.define.Checkable;
import com.vortex.vortexdb.exception.NotFoundException;
import com.vortex.api.server.RestServer;
import com.vortex.common.util.E;
import com.vortex.vortexdb.util.JsonUtil;
import com.vortex.common.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;

import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import java.util.List;
import java.util.Map;

@Path("graphs/{graph}/auth/targets")
@Singleton
public class TargetAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String create(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         JsonTarget jsonTarget) {
        LOG.debug("Graph [{}] create target: {}", graph, jsonTarget);
        checkCreatingBody(jsonTarget);

        Vortex g = graph(manager, graph);
        VortexTarget target = jsonTarget.build();
        target.id(manager.authManager().createTarget(target));
        return manager.serializer(g).writeAuthElement(target);
    }

    @PUT
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String update(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         @PathParam("id") String id,
                         JsonTarget jsonTarget) {
        LOG.debug("Graph [{}] update target: {}", graph, jsonTarget);
        checkUpdatingBody(jsonTarget);

        Vortex g = graph(manager, graph);
        VortexTarget target;
        try {
            target = manager.authManager().getTarget(UserAPI.parseId(id));
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid target id: " + id);
        }
        target = jsonTarget.build(target);
        manager.authManager().updateTarget(target);
        return manager.serializer(g).writeAuthElement(target);
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String list(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @QueryParam("limit") @DefaultValue("100") long limit) {
        LOG.debug("Graph [{}] list targets", graph);

        Vortex g = graph(manager, graph);
        List<VortexTarget> targets = manager.authManager().listAllTargets(limit);
        return manager.serializer(g).writeAuthElements("targets", targets);
    }

    @GET
    @Timed
    @Path("{id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @PathParam("id") String id) {
        LOG.debug("Graph [{}] get target: {}", graph, id);

        Vortex g = graph(manager, graph);
        VortexTarget target = manager.authManager().getTarget(UserAPI.parseId(id));
        return manager.serializer(g).writeAuthElement(target);
    }

    @DELETE
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @PathParam("id") String id) {
        LOG.debug("Graph [{}] delete target: {}", graph, id);

        @SuppressWarnings("unused") // just check if the graph exists
        Vortex g = graph(manager, graph);
        try {
            manager.authManager().deleteTarget(UserAPI.parseId(id));
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid target id: " + id);
        }
    }

    @JsonIgnoreProperties(value = {"id", "target_creator",
                                   "target_create", "target_update"})
    private static class JsonTarget implements Checkable {

        @JsonProperty("target_name")
        private String name;
        @JsonProperty("target_graph")
        private String graph;
        @JsonProperty("target_url")
        private String url;
        @JsonProperty("target_resources") // error when List<VortexResource>
        private List<Map<String, Object>> resources;

        public VortexTarget build(VortexTarget target) {
            E.checkArgument(this.name == null ||
                            target.name().equals(this.name),
                            "The name of target can't be updated");
            E.checkArgument(this.graph == null ||
                            target.graph().equals(this.graph),
                            "The graph of target can't be updated");
            if (this.url != null) {
                target.url(this.url);
            }
            if (this.resources != null) {
                target.resources(JsonUtil.toJson(this.resources));
            }
            return target;
        }

        public VortexTarget build() {
            VortexTarget target = new VortexTarget(this.name, this.graph, this.url);
            if (this.resources != null) {
                target.resources(JsonUtil.toJson(this.resources));
            }
            return target;
        }

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgumentNotNull(this.name,
                                   "The name of target can't be null");
            E.checkArgumentNotNull(this.graph,
                                   "The graph of target can't be null");
            E.checkArgumentNotNull(this.url,
                                   "The url of target can't be null");
        }

        @Override
        public void checkUpdate() {
            E.checkArgument(this.url != null ||
                            this.resources != null,
                            "Expect one of target url/resources");

        }
    }
}
