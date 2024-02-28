package com.vortex.client.api.schema;

import com.vortex.client.api.task.TaskAPI;
import com.vortex.client.client.RestClient;
import com.vortex.common.rest.RestResult;
import com.vortex.client.structure.SchemaElement;
import com.vortex.client.structure.constant.VortexType;
import com.vortex.client.structure.schema.EdgeLabel;
import com.vortex.common.util.E;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

public class EdgeLabelAPI extends SchemaElementAPI {

    public EdgeLabelAPI(RestClient client, String graph) {
        super(client, graph);
    }

    @Override
    protected String type() {
        return VortexType.EDGE_LABEL.string();
    }

    public EdgeLabel create(EdgeLabel edgeLabel) {
        Object el = this.checkCreateOrUpdate(edgeLabel);
        RestResult result = this.client.post(this.path(), el);
        return result.readObject(EdgeLabel.class);
    }

    public EdgeLabel append(EdgeLabel edgeLabel) {
        String id = edgeLabel.name();
        Map<String, Object> params = ImmutableMap.of("action", "append");
        Object el = this.checkCreateOrUpdate(edgeLabel);
        RestResult result = this.client.put(this.path(), id, el, params);
        return result.readObject(EdgeLabel.class);
    }

    public EdgeLabel eliminate(EdgeLabel edgeLabel) {
        String id = edgeLabel.name();
        Map<String, Object> params = ImmutableMap.of("action", "eliminate");
        Object el = this.checkCreateOrUpdate(edgeLabel);
        RestResult result = this.client.put(this.path(), id, el, params);
        return result.readObject(EdgeLabel.class);
    }

    public EdgeLabel get(String name) {
        RestResult result = this.client.get(this.path(), name);
        return result.readObject(EdgeLabel.class);
    }

    public List<EdgeLabel> list() {
        RestResult result = this.client.get(this.path());
        return result.readList(this.type(), EdgeLabel.class);
    }

    public List<EdgeLabel> list(List<String> names) {
        this.client.checkApiVersion("0.48", "getting schema by names");
        E.checkArgument(names != null && !names.isEmpty(),
                        "The edge label names can't be null or empty");
        Map<String, Object> params = ImmutableMap.of("names", names);
        RestResult result = this.client.get(this.path(), params);
        return result.readList(this.type(), EdgeLabel.class);
    }

    public long delete(String name) {
        RestResult result = this.client.delete(this.path(), name);
        @SuppressWarnings("unchecked")
        Map<String, Object> task = result.readObject(Map.class);
        return TaskAPI.parseTaskId(task);
    }

    @Override
    protected Object checkCreateOrUpdate(SchemaElement schemaElement) {
        EdgeLabel edgeLabel = (EdgeLabel) schemaElement;
        Object el = edgeLabel;
        if (this.client.apiVersionLt("0.54")) {
            E.checkArgument(edgeLabel.ttl() == 0L &&
                            edgeLabel.ttlStartTime() == null,
                            "Not support ttl until api version 0.54");
            el = edgeLabel.switchV53();
        }
        return el;
    }
}
