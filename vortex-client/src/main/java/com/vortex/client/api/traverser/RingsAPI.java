package com.vortex.client.api.traverser;

import com.vortex.client.api.graph.GraphAPI;
import com.vortex.client.client.RestClient;
import com.vortex.common.rest.RestResult;
import com.vortex.client.structure.constant.Direction;
import com.vortex.client.structure.graph.Path;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class RingsAPI extends TraversersAPI {

    public RingsAPI(RestClient client, String graph) {
        super(client, graph);
    }

    @Override
    protected String type() {
        return "rings";
    }

    public List<Path> get(Object sourceId, Direction direction, String label,
                          int depth, boolean sourceInRing, long degree,
                          long capacity, long limit) {
        String source = GraphAPI.formatVertexId(sourceId, false);

        checkPositive(depth, "Max depth of path");
        checkDegree(degree);
        checkCapacity(capacity);
        checkLimit(limit);

        if (sourceInRing) {
            this.client.checkApiVersion("0.40",
                                        "source_in_ring arg of ring API");
        }

        Map<String, Object> params = new LinkedHashMap<>();
        params.put("source", source);
        params.put("direction", direction);
        params.put("label", label);
        params.put("max_depth", depth);
        params.put("source_in_ring", sourceInRing);
        params.put("max_degree", degree);
        params.put("capacity", capacity);
        params.put("limit", limit);
        RestResult result = this.client.get(this.path(), params);
        return result.readList(this.type(), Path.class);
    }
}
