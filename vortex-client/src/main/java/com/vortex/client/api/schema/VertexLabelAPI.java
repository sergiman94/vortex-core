package com.vortex.client.api.schema;

import com.vortex.client.api.task.TaskAPI;
import com.vortex.client.client.RestClient;
import com.vortex.common.rest.RestResult;
import com.vortex.client.structure.SchemaElement;
import com.vortex.client.structure.constant.VortexType;
import com.vortex.client.structure.schema.VertexLabel;
import com.vortex.common.util.E;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

public class VertexLabelAPI extends SchemaElementAPI {

    public VertexLabelAPI(RestClient client, String graph) {
        super(client, graph);
    }

    @Override
    protected String type() {
        return VortexType.VERTEX_LABEL.string();
    }

    public VertexLabel create(VertexLabel vertexLabel) {
        Object vl = this.checkCreateOrUpdate(vertexLabel);
        RestResult result = this.client.post(this.path(), vl);
        return result.readObject(VertexLabel.class);
    }

    public VertexLabel append(VertexLabel vertexLabel) {
        String id = vertexLabel.name();
        Map<String, Object> params = ImmutableMap.of("action", "append");
        Object vl = this.checkCreateOrUpdate(vertexLabel);
        RestResult result = this.client.put(this.path(), id, vl, params);
        return result.readObject(VertexLabel.class);
    }

    public VertexLabel eliminate(VertexLabel vertexLabel) {
        String id = vertexLabel.name();
        Map<String, Object> params = ImmutableMap.of("action", "eliminate");
        Object vl = this.checkCreateOrUpdate(vertexLabel);
        RestResult result = this.client.put(this.path(), id, vl, params);
        return result.readObject(VertexLabel.class);
    }

    public VertexLabel get(String name) {
        RestResult result = this.client.get(this.path(), name);
        return result.readObject(VertexLabel.class);
    }

    public List<VertexLabel> list() {
        RestResult result = this.client.get(this.path());
        return result.readList(this.type(), VertexLabel.class);
    }

    public List<VertexLabel> list(List<String> names) {
        this.client.checkApiVersion("0.48", "getting schema by names");
        E.checkArgument(names != null && !names.isEmpty(),
                        "The vertex label names can't be null or empty");
        Map<String, Object> params = ImmutableMap.of("names", names);
        RestResult result = this.client.get(this.path(), params);
        return result.readList(this.type(), VertexLabel.class);
    }

    public long delete(String name) {
        RestResult result = this.client.delete(this.path(), name);
        @SuppressWarnings("unchecked")
        Map<String, Object> task = result.readObject(Map.class);
        return TaskAPI.parseTaskId(task);
    }

    @Override
    protected Object checkCreateOrUpdate(SchemaElement schemaElement) {
        VertexLabel vertexLabel = (VertexLabel) schemaElement;
        if (vertexLabel.idStrategy().isCustomizeUuid()) {
            this.client.checkApiVersion("0.46", "customize UUID strategy");
        }
        Object vl = vertexLabel;
        if (this.client.apiVersionLt("0.54")) {
            E.checkArgument(vertexLabel.ttl() == 0L &&
                            vertexLabel.ttlStartTime() == null,
                            "Not support ttl until api version 0.54");
            vl = vertexLabel.switchV53();
        }
        return vl;
    }
}
