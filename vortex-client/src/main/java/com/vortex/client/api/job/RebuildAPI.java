package com.vortex.client.api.job;

import com.vortex.client.api.task.TaskAPI;
import com.vortex.client.client.RestClient;
import com.vortex.common.rest.RestResult;
import com.vortex.client.structure.SchemaElement;
import com.vortex.client.structure.schema.EdgeLabel;
import com.vortex.client.structure.schema.IndexLabel;
import com.vortex.client.structure.schema.VertexLabel;
import com.vortex.common.util.E;

import java.util.Map;

public class RebuildAPI extends JobAPI {

    private static final String JOB_TYPE = "rebuild";

    public RebuildAPI(RestClient client, String graph) {
        super(client, graph);
    }

    @Override
    protected String jobType() {
        return JOB_TYPE;
    }

    public long rebuild(VertexLabel vertexLabel) {
        return this.rebuildIndex(vertexLabel);
    }

    public long rebuild(EdgeLabel edgeLabel) {
        return this.rebuildIndex(edgeLabel);
    }

    public long rebuild(IndexLabel indexLabel) {
        return this.rebuildIndex(indexLabel);
    }

    private long rebuildIndex(SchemaElement element) {
        E.checkArgument(element instanceof VertexLabel ||
                        element instanceof EdgeLabel ||
                        element instanceof IndexLabel,
                        "Only VertexLabel, EdgeLabel and IndexLabel support " +
                        "rebuild, but got '%s'", element);
        String path = String.join(PATH_SPLITOR, this.path(), element.type());
        RestResult result = this.client.put(path, element.name(), element);
        @SuppressWarnings("unchecked")
        Map<String, Object> task = result.readObject(Map.class);
        return TaskAPI.parseTaskId(task);
    }
}
