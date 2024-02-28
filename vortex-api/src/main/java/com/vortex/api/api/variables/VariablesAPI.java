
package com.vortex.api.api.variables;

import com.vortex.vortexdb.Vortex;
import com.vortex.api.api.API;
import com.vortex.api.core.GraphManager;
import com.vortex.api.server.RestServer;
import com.vortex.common.util.E;
import com.vortex.common.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;

import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import java.util.Map;
import java.util.Optional;

@Path("graphs/{graph}/variables")
@Singleton
public class VariablesAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @PUT
    @Timed
    @Path("{key}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, Object> update(@Context GraphManager manager,
                                      @PathParam("graph") String graph,
                                      @PathParam("key") String key,
                                      JsonVariableValue value) {
        E.checkArgument(value != null && value.data != null,
                        "The variable value can't be empty");
        LOG.debug("Graph [{}] set variable for {}: {}", graph, key, value);

        Vortex g = graph(manager, graph);
        commit(g, () -> g.variables().set(key, value.data));
        return ImmutableMap.of(key, value.data);
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, Object> list(@Context GraphManager manager,
                                    @PathParam("graph") String graph) {
        LOG.debug("Graph [{}] get variables", graph);

        Vortex g = graph(manager, graph);
        return g.variables().asMap();
    }

    @GET
    @Timed
    @Path("{key}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, Object> get(@Context GraphManager manager,
                                   @PathParam("graph") String graph,
                                   @PathParam("key") String key) {
        LOG.debug("Graph [{}] get variable by key '{}'", graph, key);

        Vortex g = graph(manager, graph);
        Optional<?> object = g.variables().get(key);
        if (!object.isPresent()) {
            throw new NotFoundException(String.format(
                      "Variable '%s' does not exist", key));
        }
        return ImmutableMap.of(key, object.get());
    }

    @DELETE
    @Timed
    @Path("{key}")
    @Consumes(APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @PathParam("key") String key) {
        LOG.debug("Graph [{}] remove variable by key '{}'", graph, key);

        Vortex g = graph(manager, graph);
        commit(g, () -> g.variables().remove(key));
    }

    private static class JsonVariableValue {

        public Object data;

        @Override
        public String toString() {
            return String.format("JsonVariableValue{data=%s}", this.data);
        }
    }
}
