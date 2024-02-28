
package com.vortex.api.api.schema;

import com.vortex.vortexdb.Vortex;
import com.vortex.api.api.API;
import com.vortex.api.api.filter.StatusFilter.Status;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.api.core.GraphManager;
import com.vortex.api.define.Checkable;
import com.vortex.vortexdb.schema.IndexLabel;
import com.vortex.vortexdb.schema.SchemaElement;
import com.vortex.vortexdb.schema.Userdata;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.vortexdb.type.define.GraphMode;
import com.vortex.vortexdb.type.define.IndexType;
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

@Path("graphs/{graph}/schema/indexlabels")
@Singleton
public class IndexLabelAPI extends API {

    private static final Logger LOG = Log.logger(IndexLabelAPI.class);

    @POST
    @Timed
    @Status(Status.ACCEPTED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$graph $action=index_label_write"})
    public String create(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         JsonIndexLabel jsonIndexLabel) {
        LOG.debug("Graph [{}] create index label: {}", graph, jsonIndexLabel);
        checkCreatingBody(jsonIndexLabel);

        Vortex g = graph(manager, graph);
        IndexLabel.Builder builder = jsonIndexLabel.convert2Builder(g);
        SchemaElement.TaskWithSchema il = builder.createWithTask();
        il.indexLabel(mapIndexLabel(il.indexLabel()));
        return manager.serializer(g).writeTaskWithSchema(il);
    }

    @PUT
    @Timed
    @Path("{name}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String update(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         @PathParam("name") String name,
                         @QueryParam("action") String action,
                         JsonIndexLabel jsonIndexLabel) {
        LOG.debug("Graph [{}] {} index label: {}",
                  graph, action, jsonIndexLabel);
        checkUpdatingBody(jsonIndexLabel);
        E.checkArgument(name.equals(jsonIndexLabel.name),
                        "The name in url(%s) and body(%s) are different",
                        name, jsonIndexLabel.name);
        // Parse action parameter
        boolean append = checkAndParseAction(action);

        Vortex g = graph(manager, graph);
        IndexLabel.Builder builder = jsonIndexLabel.convert2Builder(g);
        IndexLabel IndexLabel = append ? builder.append() : builder.eliminate();
        return manager.serializer(g).writeIndexlabel(mapIndexLabel(IndexLabel));
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$graph $action=index_label_read"})
    public String list(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @QueryParam("names") List<String> names) {
        boolean listAll = CollectionUtils.isEmpty(names);
        if (listAll) {
            LOG.debug("Graph [{}] list index labels", graph);
        } else {
            LOG.debug("Graph [{}] get index labels by names {}", graph, names);
        }

        Vortex g = graph(manager, graph);
        List<IndexLabel> labels;
        if (listAll) {
            labels = g.schema().getIndexLabels();
        } else {
            labels = new ArrayList<>(names.size());
            for (String name : names) {
                labels.add(g.schema().getIndexLabel(name));
            }
        }
        return manager.serializer(g).writeIndexlabels(mapIndexLabels(labels));
    }

    @GET
    @Timed
    @Path("{name}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$graph $action=index_label_read"})
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @PathParam("name") String name) {
        LOG.debug("Graph [{}] get index label by name '{}'", graph, name);

        Vortex g = graph(manager, graph);
        IndexLabel indexLabel = g.schema().getIndexLabel(name);
        return manager.serializer(g).writeIndexlabel(mapIndexLabel(indexLabel));
    }

    @DELETE
    @Timed
    @Path("{name}")
    @Status(Status.ACCEPTED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$graph $action=index_label_delete"})
    public Map<String, Id> delete(@Context GraphManager manager,
                                  @PathParam("graph") String graph,
                                  @PathParam("name") String name) {
        LOG.debug("Graph [{}] remove index label by name '{}'", graph, name);

        Vortex g = graph(manager, graph);
        // Throw 404 if not exists
        g.schema().getIndexLabel(name);
        return ImmutableMap.of("task_id",
                               g.schema().indexLabel(name).remove());
    }

    private static List<IndexLabel> mapIndexLabels(List<IndexLabel> labels) {
        List<IndexLabel> results = new ArrayList<>(labels.size());
        for (IndexLabel il : labels) {
            results.add(mapIndexLabel(il));
        }
        return results;
    }

    /**
     * Map RANGE_INT/RANGE_FLOAT/RANGE_LONG/RANGE_DOUBLE to RANGE
     */
    private static IndexLabel mapIndexLabel(IndexLabel label) {
        if (label.indexType().isRange()) {
            label = (IndexLabel) label.copy();
            label.indexType(IndexType.RANGE);
        }
        return label;
    }

    /**
     * JsonIndexLabel is only used to receive create and append requests
     */
    @JsonIgnoreProperties(value = {"status"})
    private static class JsonIndexLabel implements Checkable {

        @JsonProperty("id")
        public long id;
        @JsonProperty("name")
        public String name;
        @JsonProperty("base_type")
        public VortexType baseType;
        @JsonProperty("base_value")
        public String baseValue;
        @JsonProperty("index_type")
        public IndexType indexType;
        @JsonProperty("fields")
        public String[] fields;
        @JsonProperty("user_data")
        public Userdata userdata;
        @JsonProperty("check_exist")
        public Boolean checkExist;
        @JsonProperty("rebuild")
        public Boolean rebuild;

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgumentNotNull(this.name,
                                   "The name of index label can't be null");
            E.checkArgumentNotNull(this.baseType,
                                   "The base type of index label '%s' " +
                                   "can't be null", this.name);
            E.checkArgument(this.baseType == VortexType.VERTEX_LABEL ||
                            this.baseType == VortexType.EDGE_LABEL,
                            "The base type of index label '%s' can only be " +
                            "either VERTEX_LABEL or EDGE_LABEL", this.name);
            E.checkArgumentNotNull(this.baseValue,
                                   "The base value of index label '%s' " +
                                   "can't be null", this.name);
            E.checkArgumentNotNull(this.indexType,
                                   "The index type of index label '%s' " +
                                   "can't be null", this.name);
        }

        @Override
        public void checkUpdate() {
            E.checkArgumentNotNull(this.name,
                                   "The name of index label can't be null");
            E.checkArgument(this.baseType == null,
                            "The base type of index label '%s' must be null",
                            this.name);
            E.checkArgument(this.baseValue == null,
                            "The base value of index label '%s' must be null",
                            this.name);
            E.checkArgument(this.indexType == null,
                            "The index type of index label '%s' must be null",
                            this.name);
        }

        private IndexLabel.Builder convert2Builder(Vortex g) {
            IndexLabel.Builder builder = g.schema().indexLabel(this.name);
            if (this.id != 0) {
                E.checkArgument(this.id > 0,
                                "Only positive number can be assign as " +
                                "index label id");
                E.checkArgument(g.mode() == GraphMode.RESTORING,
                                "Only accept index label id when graph in " +
                                "RESTORING mode, but '%s' is in mode '%s'",
                                g, g.mode());
                builder.id(this.id);
            }
            if (this.baseType != null) {
                assert this.baseValue != null;
                builder.on(this.baseType, this.baseValue);
            }
            if (this.indexType != null) {
                builder.indexType(this.indexType);
            }
            if (this.fields != null && this.fields.length > 0) {
                builder.by(this.fields);
            }
            if (this.userdata != null) {
                builder.userdata(this.userdata);
            }
            if (this.checkExist != null) {
                builder.checkExist(this.checkExist);
            }
            if (this.rebuild != null) {
                builder.rebuild(this.rebuild);
            }
            return builder;
        }

        @Override
        public String toString() {
            return String.format("JsonIndexLabel{name=%s, baseType=%s," +
                                 "baseValue=%s, indexType=%s, fields=%s}",
                                 this.name, this.baseType, this.baseValue,
                                 this.indexType, this.fields);
        }
    }
}
