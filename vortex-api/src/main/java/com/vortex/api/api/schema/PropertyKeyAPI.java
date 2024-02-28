
package com.vortex.api.api.schema;

import com.vortex.vortexdb.Vortex;
import com.vortex.api.api.API;
import com.vortex.api.api.filter.StatusFilter.Status;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.id.IdGenerator;
import com.vortex.api.core.GraphManager;
import com.vortex.api.define.Checkable;
import com.vortex.vortexdb.schema.PropertyKey;
import com.vortex.vortexdb.schema.SchemaElement;
import com.vortex.vortexdb.schema.Userdata;
import com.vortex.vortexdb.type.define.*;
import com.vortex.common.util.E;
import com.vortex.common.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;

import javax.annotation.security.RolesAllowed;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Path("graphs/{graph}/schema/propertykeys")
@Singleton
public class PropertyKeyAPI extends API {

    private static final Logger LOG = Log.logger(PropertyKeyAPI.class);

    @POST
    @Timed
    @Status(Status.ACCEPTED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$graph $action=property_key_write"})
    public String create(@Context GraphManager manager, @PathParam("graph") String graph, JsonPropertyKey jsonPropertyKey) {
        LOG.debug("Graph [{}] create property key: {}",
                  graph, jsonPropertyKey);
        checkCreatingBody(jsonPropertyKey);

        Vortex g = graph(manager, graph);
        PropertyKey.Builder builder = jsonPropertyKey.convert2Builder(g);
        SchemaElement.TaskWithSchema pk = builder.createWithTask();
        return manager.serializer(g).writeTaskWithSchema(pk);
    }

    @PUT
    @Timed
    @Status(Status.ACCEPTED)
    @Path("{name}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$graph $action=property_key_write"})
    public String update(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         @PathParam("name") String name,
                         @QueryParam("action") String action,
                         JsonPropertyKey jsonPropertyKey) {
        LOG.debug("Graph [{}] {} property key: {}",
                  graph, action, jsonPropertyKey);
        checkUpdatingBody(jsonPropertyKey);
        E.checkArgument(name.equals(jsonPropertyKey.name),
                        "The name in url(%s) and body(%s) are different",
                        name, jsonPropertyKey.name);

        Vortex g = graph(manager, graph);
        if (ACTION_CLEAR.equals(action)) {
            PropertyKey propertyKey = g.propertyKey(name);
            E.checkArgument(propertyKey.olap(),
                            "Only olap property key can do action clear, " +
                            "but got '%s'", propertyKey);
            Id id = g.clearPropertyKey(propertyKey);
            SchemaElement.TaskWithSchema pk =
                    new SchemaElement.TaskWithSchema(propertyKey, id);
            return manager.serializer(g).writeTaskWithSchema(pk);
        }

        // Parse action parameter
        boolean append = checkAndParseAction(action);

        PropertyKey.Builder builder = jsonPropertyKey.convert2Builder(g);
        PropertyKey propertyKey = append ?
                                  builder.append() :
                                  builder.eliminate();
        SchemaElement.TaskWithSchema pk =
                new SchemaElement.TaskWithSchema(propertyKey, IdGenerator.ZERO);
        return manager.serializer(g).writeTaskWithSchema(pk);
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$graph $action=property_key_read"})
    public String list(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @QueryParam("names") List<String> names) {
        boolean listAll = CollectionUtils.isEmpty(names);
        if (listAll) {
            LOG.debug("Graph [{}] list property keys", graph);
        } else {
            LOG.debug("Graph [{}] get property keys by names {}", graph, names);
        }

        Vortex g = graph(manager, graph);
        List<PropertyKey> propKeys;
        if (listAll) {
            propKeys = g.schema().getPropertyKeys();
        } else {
            propKeys = new ArrayList<>(names.size());
            for (String name : names) {
                propKeys.add(g.schema().getPropertyKey(name));
            }
        }
        return manager.serializer(g).writePropertyKeys(propKeys);
    }

    @GET
    @Timed
    @Path("{name}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$graph $action=property_key_read"})
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @PathParam("name") String name) {
        LOG.debug("Graph [{}] get property key by name '{}'", graph, name);

        Vortex g = graph(manager, graph);
        PropertyKey propertyKey = g.schema().getPropertyKey(name);
        return manager.serializer(g).writePropertyKey(propertyKey);
    }

    @DELETE
    @Timed
    @Status(Status.ACCEPTED)
    @Path("{name}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$graph $action=property_key_delete"})
    public Map<String, Id> delete(@Context GraphManager manager,
                                  @PathParam("graph") String graph,
                                  @PathParam("name") String name) {
        LOG.debug("Graph [{}] remove property key by name '{}'", graph, name);

        Vortex g = graph(manager, graph);
        // Throw 404 if not exists
        g.schema().getPropertyKey(name);
        return ImmutableMap.of("task_id",
                               g.schema().propertyKey(name).remove());
    }

    /**
     * JsonPropertyKey is only used to receive create and append requests
     */
    @JsonIgnoreProperties(value = {"status"})
    private static class JsonPropertyKey implements Checkable {

        @JsonProperty("id")
        public long id;
        @JsonProperty("name")
        public String name;
        @JsonProperty("cardinality")
        public Cardinality cardinality;
        @JsonProperty("data_type")
        public DataType dataType;
        @JsonProperty("aggregate_type")
        public AggregateType aggregateType;
        @JsonProperty("write_type")
        public WriteType writeType;
        @JsonProperty("properties")
        public String[] properties;
        @JsonProperty("user_data")
        public Userdata userdata;
        @JsonProperty("check_exist")
        public Boolean checkExist;

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgumentNotNull(this.name,
                                   "The name of property key can't be null");
            E.checkArgument(this.properties == null ||
                            this.properties.length == 0,
                            "Not allowed to pass properties when " +
                            "creating property key since it doesn't " +
                            "support meta properties currently");
        }

        private PropertyKey.Builder convert2Builder(Vortex g) {
            PropertyKey.Builder builder = g.schema().propertyKey(this.name);
            if (this.id != 0) {
                E.checkArgument(this.id > 0,
                                "Only positive number can be assign as " +
                                "property key id");
                E.checkArgument(g.mode() == GraphMode.RESTORING,
                                "Only accept property key id when graph in " +
                                "RESTORING mode, but '%s' is in mode '%s'",
                                g, g.mode());
                builder.id(this.id);
            }
            if (this.cardinality != null) {
                builder.cardinality(this.cardinality);
            }
            if (this.dataType != null) {
                builder.dataType(this.dataType);
            }
            if (this.aggregateType != null) {
                builder.aggregateType(this.aggregateType);
            }
            if (this.writeType != null) {
                builder.writeType(this.writeType);
            }
            if (this.userdata != null) {
                builder.userdata(this.userdata);
            }
            if (this.checkExist != null) {
                builder.checkExist(this.checkExist);
            }
            return builder;
        }

        @Override
        public String toString() {
            return String.format("JsonPropertyKey{name=%s, cardinality=%s, " +
                                 "dataType=%s, aggregateType=%s, " +
                                 "writeType=%s, properties=%s}",
                                 this.name, this.cardinality,
                                 this.dataType, this.aggregateType,
                                 this.writeType, this.properties);
        }
    }
}
