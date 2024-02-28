package com.vortex.client.api.traverser;

import com.vortex.client.api.graph.GraphAPI;
import com.vortex.client.client.RestClient;
import com.vortex.common.rest.RestResult;
import com.vortex.client.structure.constant.Direction;
import com.vortex.client.structure.graph.Path;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ShortestPathAPI extends TraversersAPI {

    public ShortestPathAPI(RestClient client, String graph) {
        super(client, graph);
    }

    @Override
    protected String type() {
        return "shortestpath";
    }

    public Path get(Object sourceId, Object targetId,
                    Direction direction, String label, int maxDepth,
                    long degree, long skipDegree, long capacity) {
        String source = GraphAPI.formatVertexId(sourceId, false);
        String target = GraphAPI.formatVertexId(targetId, false);

        checkPositive(maxDepth, "Max depth of shortest path");
        checkDegree(degree);
        checkCapacity(capacity);
        checkSkipDegree(skipDegree, degree, capacity);

        Map<String, Object> params = new LinkedHashMap<>();
        params.put("source", source);
        params.put("target", target);
        params.put("direction", direction);
        params.put("label", label);
        params.put("max_depth", maxDepth);
        params.put("max_degree", degree);
        params.put("skip_degree", skipDegree);
        params.put("capacity", capacity);
        RestResult result = this.client.get(this.path(), params);
        List<Object> vertices = result.readList("path", Object.class);
        return new Path(vertices);
    }
}
