package com.vortex.client.api.traverser;

import com.vortex.client.api.graph.GraphAPI;
import com.vortex.client.client.RestClient;
import com.vortex.common.rest.RestResult;
import com.vortex.client.structure.constant.Direction;
import com.vortex.client.structure.traverser.WeightedPaths;
import com.vortex.common.util.E;

import java.util.LinkedHashMap;
import java.util.Map;

public class SingleSourceShortestPathAPI extends TraversersAPI {

    public SingleSourceShortestPathAPI(RestClient client, String graph) {
        super(client, graph);
    }

    @Override
    protected String type() {
        return "singlesourceshortestpath";
    }

    public WeightedPaths get(Object sourceId, Direction direction, String label,
                             String weight, long degree, long skipDegree,
                             long capacity, long limit, boolean withVertex) {
        this.client.checkApiVersion("0.51", "single source shortest path");
        String source = GraphAPI.formatVertexId(sourceId, false);

        E.checkNotNull(weight, "weight");
        checkDegree(degree);
        checkCapacity(capacity);
        checkSkipDegree(skipDegree, degree, capacity);
        checkLimit(limit);

        Map<String, Object> params = new LinkedHashMap<>();
        params.put("source", source);
        params.put("direction", direction);
        params.put("label", label);
        params.put("weight", weight);
        params.put("max_degree", degree);
        params.put("skip_degree", skipDegree);
        params.put("capacity", capacity);
        params.put("limit", limit);
        params.put("with_vertex", withVertex);
        RestResult result = this.client.get(this.path(), params);
        return result.readObject(WeightedPaths.class);
    }
}
