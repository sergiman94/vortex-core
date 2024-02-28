
package com.vortex.api.api.schema;

import com.vortex.vortexdb.Vortex;
import com.vortex.api.api.API;
import com.vortex.api.core.GraphManager;
import com.vortex.vortexdb.schema.SchemaManager;
import com.vortex.common.util.Log;
import com.codahale.metrics.annotation.Timed;
import org.slf4j.Logger;

import javax.annotation.security.RolesAllowed;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Path("graphs/{graph}/schema")
@Singleton
public class SchemaAPI extends API {

    private static final Logger LOG = Log.logger(SchemaAPI.class);

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$graph $action=schema_read"})
    public String list(@Context GraphManager manager,
                       @PathParam("graph") String graph) {
        LOG.debug("Graph [{}] list all schema", graph);

        Vortex g = graph(manager, graph);
        SchemaManager schema = g.schema();

        Map<String, List<?>> schemaMap = new LinkedHashMap<>(4);
        schemaMap.put("propertykeys", schema.getPropertyKeys());
        schemaMap.put("vertexlabels", schema.getVertexLabels());
        schemaMap.put("edgelabels", schema.getEdgeLabels());
        schemaMap.put("indexlabels", schema.getIndexLabels());

        return manager.serializer(g).writeMap(schemaMap);
    }
}
