
package com.vortex.api.api.profile;

import com.google.gson.*;
import com.vortex.vortexdb.Vortex;
import com.vortex.api.api.API;
import com.vortex.api.auth.VortexAuthenticator.RequiredPerm;
import com.vortex.vortexdb.auth.VortexPermission;
import com.vortex.common.config.VortexConfig;
import com.vortex.api.core.GraphManager;
import com.vortex.api.server.RestServer;
import com.vortex.vortexdb.type.define.GraphMode;
import com.vortex.vortexdb.type.define.GraphReadMode;
import com.vortex.common.util.E;
import com.vortex.vortexdb.util.JsonUtil;
import com.vortex.common.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import javax.annotation.security.RolesAllowed;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.SecurityContext;
import java.io.File;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Path("graphs")
@Singleton
public class GraphsAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    private static final String CONFIRM_CLEAR = "I'm sure to delete all data";
    private static final String CONFIRM_DROP = "I'm sure to drop the graph";

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$dynamic"})
    public Object list(@Context GraphManager manager,
                       @Context SecurityContext sc) {
        Set<String> graphs = manager.graphs();
        // Filter by user role
        Set<String> filterGraphs = new HashSet<>();
        for (String graph : graphs) {
            String role = RequiredPerm.roleFor(graph, VortexPermission.READ);
            if (sc.isUserInRole(role)) {
                try {
                    Vortex g = graph(manager, graph);
                    filterGraphs.add(g.name());
                } catch (ForbiddenException ignored) {
                    // ignore
                }
            }
        }
        return ImmutableMap.of("graphs", filterGraphs);
    }

    @GET
    @Timed
    @Path("{name}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$name"})
    public Object get(@Context GraphManager manager,
                      @PathParam("name") String name) {
        LOG.debug("Get graph by name '{}'", name);

        Vortex g = graph(manager, name);
        return ImmutableMap.of("name", g.name(), "backend", g.backend());
    }

    @DELETE
    @Timed
    @Path("{name}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$name"})
    public void drop(@Context GraphManager manager,
                     @PathParam("name") String name,
                     @QueryParam("confirm_message") String message) {
        LOG.debug("Drop graph by name '{}'", name);

        E.checkArgument(CONFIRM_DROP.equals(message),
                        "Please take the message: %s", CONFIRM_DROP);
        manager.dropGraph(name);
    }

    @POST
    @Timed
    @Path("{name}")
    @Consumes(APPLICATION_JSON_WITH_CHARSET)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public Object create(@Context GraphManager manager, @PathParam("name") String name, @QueryParam("clone_graph_name") String clone, String configText) {

        JsonParser parser = new JsonParser();
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        JsonElement el = parser.parse(configText);
        String jsonString = gson.toJson(el);
        String cleanConfigText = jsonString.replace(",", "").replaceAll("\"", "");

        LOG.info("Create graph '{}' with clone graph '{}', config text '{}'", name, clone, cleanConfigText);

        Vortex graph;
        if (StringUtils.isNotEmpty(clone)) {
            graph = manager.cloneGraph(clone, name, cleanConfigText);
        } else {
            graph = manager.createGraph(name, cleanConfigText);
        }
        return ImmutableMap.of("name", graph.name(),
                               "backend", graph.backend());
    }

    @GET
    @Timed
    @Path("{name}/conf")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed("admin")
    public File getConf(@Context GraphManager manager,
                        @PathParam("name") String name) {
        LOG.debug("Get graph configuration by name '{}'", name);

        Vortex g = graph4admin(manager, name);

        VortexConfig config = (VortexConfig) g.configuration();
        File file = config.getFile();
        if (file == null) {
            throw new NotSupportedException("Can't access the api in " +
                      "a node which started with non local file config.");
        }
        return file;
    }

    @DELETE
    @Timed
    @Path("{name}/clear")
    @Consumes(APPLICATION_JSON)
    @RolesAllowed("admin")
    public void clear(@Context GraphManager manager,
                      @PathParam("name") String name,
                      @QueryParam("confirm_message") String message) {
        LOG.debug("Clear graph by name '{}'", name);

        E.checkArgument(CONFIRM_CLEAR.equals(message),
                        "Please take the message: %s", CONFIRM_CLEAR);
        Vortex g = graph(manager, name);
        g.truncateBackend();
    }

    @PUT
    @Timed
    @Path("{name}/snapshot_create")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$name"})
    public Object createSnapshot(@Context GraphManager manager,
                                 @PathParam("name") String name) {
        LOG.debug("Create snapshot for graph '{}'", name);

        Vortex g = graph(manager, name);
        g.createSnapshot();
        return ImmutableMap.of(name, "snapshot_created");
    }

    @PUT
    @Timed
    @Path("{name}/snapshot_resume")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$name"})
    public Object resumeSnapshot(@Context GraphManager manager,
                                 @PathParam("name") String name) {
        LOG.debug("Resume snapshot for graph '{}'", name);

        Vortex g = graph(manager, name);
        g.resumeSnapshot();
        return ImmutableMap.of(name, "snapshot_resumed");
    }

    @PUT
    @Timed
    @Path("{name}/compact")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public String compact(@Context GraphManager manager,
                          @PathParam("name") String name) {
        LOG.debug("Manually compact graph '{}'", name);

        Vortex g = graph(manager, name);
        return JsonUtil.toJson(g.metadata(null, "compact"));
    }

    @PUT
    @Timed
    @Path("{name}/mode")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$name"})
    public Map<String, GraphMode> mode(@Context GraphManager manager,
                                       @PathParam("name") String name,
                                       GraphMode mode) {
        LOG.debug("Set mode to: '{}' of graph '{}'", mode, name);

        E.checkArgument(mode != null, "Graph mode can't be null");
        Vortex g = graph(manager, name);
        g.mode(mode);
        return ImmutableMap.of("mode", mode);
    }

    @GET
    @Timed
    @Path("{name}/mode")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$name"})
    public Map<String, GraphMode> mode(@Context GraphManager manager,
                                       @PathParam("name") String name) {
        LOG.debug("Get mode of graph '{}'", name);

        Vortex g = graph(manager, name);
        return ImmutableMap.of("mode", g.mode());
    }

    @PUT
    @Timed
    @Path("{name}/graph_read_mode")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed("admin")
    public Map<String, GraphReadMode> graphReadMode(
                                      @Context GraphManager manager,
                                      @PathParam("name") String name,
                                      GraphReadMode readMode) {
        LOG.debug("Set graph-read-mode to: '{}' of graph '{}'",
                  readMode, name);

        E.checkArgument(readMode != null,
                        "Graph-read-mode can't be null");
        Vortex g = graph(manager, name);
        g.readMode(readMode);
        return ImmutableMap.of("graph_read_mode", readMode);
    }

    @GET
    @Timed
    @Path("{name}/graph_read_mode")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$name"})
    public Map<String, GraphReadMode> graphReadMode(
                                      @Context GraphManager manager,
                                      @PathParam("name") String name) {
        LOG.debug("Get graph-read-mode of graph '{}'", name);

        Vortex g = graph(manager, name);
        return ImmutableMap.of("graph_read_mode", g.readMode());
    }
}
