package com.vortex.client.api.traverser;

import com.vortex.client.api.graph.GraphAPI;
import com.vortex.client.client.RestClient;
import com.vortex.common.rest.RestResult;
import com.vortex.client.structure.constant.Direction;
import com.vortex.client.structure.traverser.Kout;
import com.vortex.client.structure.traverser.KoutRequest;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class KoutAPI extends TraversersAPI {

    public KoutAPI(RestClient client, String graph) {
        super(client, graph);
    }

    @Override
    protected String type() {
        return "kout";
    }

    public List<Object> get(Object sourceId, Direction direction,
                            String label, int depth, boolean nearest,
                            long degree, long capacity, long limit) {
        String source = GraphAPI.formatVertexId(sourceId, false);

        checkPositive(depth, "Depth of k-out");
        checkDegree(degree);
        checkCapacity(capacity);
        checkLimit(limit);

        Map<String, Object> params = new LinkedHashMap<>();
        params.put("source", source);
        params.put("direction", direction);
        params.put("label", label);
        params.put("max_depth", depth);
        params.put("nearest", nearest);
        params.put("max_degree", degree);
        params.put("capacity", capacity);
        params.put("limit", limit);
        RestResult result = this.client.get(this.path(), params);
        return result.readList("vertices", Object.class);
    }

    public Kout post(KoutRequest request) {
        this.client.checkApiVersion("0.58", "customized kout");
        RestResult result = this.client.post(this.path(), request);
        return result.readObject(Kout.class);
    }
}

