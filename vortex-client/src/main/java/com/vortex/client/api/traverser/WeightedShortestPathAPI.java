package com.vortex.client.api.traverser;

import com.vortex.client.api.graph.GraphAPI;
import com.vortex.client.client.RestClient;
import com.vortex.common.rest.RestResult;
import com.vortex.client.structure.constant.Direction;
import com.vortex.client.structure.traverser.WeightedPath;
import com.vortex.common.util.E;

import java.util.LinkedHashMap;
import java.util.Map;

public class WeightedShortestPathAPI extends TraversersAPI {

    public WeightedShortestPathAPI(RestClient client, String graph) {
        super(client, graph);
    }

    @Override
    protected String type() {
        return "weightedshortestpath";
    }

    public WeightedPath get(Object sourceId, Object targetId,
                            Direction direction, String label,
                            String weight, long degree, long skipDegree,
                            long capacity, boolean withVertex) {
        this.client.checkApiVersion("0.51", "weighted shortest path");
        String source = GraphAPI.formatVertexId(sourceId, false);
        String target = GraphAPI.formatVertexId(targetId, false);

        E.checkNotNull(weight, "weight");
        checkDegree(degree);
        checkCapacity(capacity);
        checkSkipDegree(skipDegree, degree, capacity);

        Map<String, Object> params = new LinkedHashMap<>();
        params.put("source", source);
        params.put("target", target);
        params.put("direction", direction);
        params.put("label", label);
        params.put("weight", weight);
        params.put("max_degree", degree);
        params.put("skip_degree", skipDegree);
        params.put("capacity", capacity);
        params.put("with_vertex", withVertex);
        RestResult result = this.client.get(this.path(), params);
        return result.readObject(WeightedPath.class);
    }
}
