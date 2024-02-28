package com.vortex.client.api.traverser;

import com.vortex.client.api.graph.GraphAPI;
import com.vortex.client.client.RestClient;
import com.vortex.common.rest.RestResult;
import com.vortex.client.structure.constant.Direction;
import com.vortex.client.structure.graph.Path;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class CrosspointsAPI extends TraversersAPI {

    public CrosspointsAPI(RestClient client, String graph) {
        super(client, graph);
    }

    @Override
    protected String type() {
        return "crosspoints";
    }

    public List<Path> get(Object sourceId, Object targetId,
                          Direction direction, String label,
                          int maxDepth, long degree,
                          long capacity, long limit) {
        String source = GraphAPI.formatVertexId(sourceId, false);
        String target = GraphAPI.formatVertexId(targetId, false);

        checkPositive(maxDepth, "Max depth of path");
        checkDegree(degree);
        checkCapacity(capacity);
        checkLimit(limit);

        Map<String, Object> params = new LinkedHashMap<>();
        params.put("source", source);
        params.put("target", target);
        params.put("direction", direction);
        params.put("label", label);
        params.put("max_depth", maxDepth);
        params.put("max_degree", degree);
        params.put("capacity", capacity);
        params.put("limit", limit);
        RestResult result = this.client.get(this.path(), params);
        return result.readList("crosspoints", Path.class);
    }
}
