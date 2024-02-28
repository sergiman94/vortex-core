package com.vortex.client.api.traverser;

import com.vortex.client.api.graph.GraphAPI;
import com.vortex.client.client.RestClient;
import com.vortex.common.rest.RestResult;
import com.vortex.client.structure.constant.Direction;
import com.vortex.client.structure.traverser.Kneighbor;
import com.vortex.client.structure.traverser.KneighborRequest;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class KneighborAPI extends TraversersAPI {

    public KneighborAPI(RestClient client, String graph) {
        super(client, graph);
    }

    @Override
    protected String type() {
        return "kneighbor";
    }

    public List<Object> get(Object sourceId, Direction direction,
                            String label, int depth, long degree, long limit) {
        String source = GraphAPI.formatVertexId(sourceId, false);

        checkPositive(depth, "Depth of k-neighbor");
        checkDegree(degree);
        checkLimit(limit);

        Map<String, Object> params = new LinkedHashMap<>();
        params.put("source", source);
        params.put("direction", direction);
        params.put("label", label);
        params.put("max_depth", depth);
        params.put("max_degree", degree);
        params.put("limit", limit);
        RestResult result = this.client.get(this.path(), params);
        return result.readList("vertices", Object.class);
    }

    public Kneighbor post(KneighborRequest request) {
        this.client.checkApiVersion("0.58", "customized kneighbor");
        RestResult result = this.client.post(this.path(), request);
        return result.readObject(Kneighbor.class);
    }
}

