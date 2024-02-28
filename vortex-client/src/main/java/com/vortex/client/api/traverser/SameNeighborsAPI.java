package com.vortex.client.api.traverser;

import com.vortex.client.api.graph.GraphAPI;
import com.vortex.client.client.RestClient;
import com.vortex.common.rest.RestResult;
import com.vortex.client.structure.constant.Direction;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SameNeighborsAPI extends TraversersAPI {

    private static final String SAME_NEIGHBORS = "same_neighbors";

    public SameNeighborsAPI(RestClient client, String graph) {
        super(client, graph);
    }

    @Override
    protected String type() {
        return "sameneighbors";
    }

    public List<Object> get(Object vertexId, Object otherId,
                            Direction direction, String label,
                            long degree, long limit) {
        this.client.checkApiVersion("0.51", "same neighbors");
        String vertex = GraphAPI.formatVertexId(vertexId, false);
        String other = GraphAPI.formatVertexId(otherId, false);
        checkDegree(degree);
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("vertex", vertex);
        params.put("other", other);
        params.put("direction", direction);
        params.put("label", label);
        params.put("max_degree", degree);
        params.put("limit", limit);
        RestResult result = this.client.get(this.path(), params);
        return result.readList(SAME_NEIGHBORS, Object.class);
    }
}
