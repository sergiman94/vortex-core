package com.vortex.client.api.traverser;

import com.vortex.client.api.graph.GraphAPI;
import com.vortex.client.client.RestClient;
import com.vortex.common.rest.RestResult;
import com.vortex.client.structure.constant.Direction;
import com.vortex.client.structure.traverser.SingleSourceJaccardSimilarityRequest;
import com.vortex.common.util.E;

import java.util.LinkedHashMap;
import java.util.Map;

public class JaccardSimilarityAPI extends TraversersAPI {

    private static final String JACCARD_SIMILARITY = "jaccard_similarity";

    public JaccardSimilarityAPI(RestClient client, String graph) {
        super(client, graph);
    }

    @Override
    protected String type() {
        return "jaccardsimilarity";
    }

    public double get(Object vertexId, Object otherId, Direction direction,
                      String label, long degree) {
        this.client.checkApiVersion("0.51", "jaccard similarity");
        String vertex = GraphAPI.formatVertexId(vertexId, false);
        String other = GraphAPI.formatVertexId(otherId, false);
        checkDegree(degree);
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("vertex", vertex);
        params.put("other", other);
        params.put("direction", direction);
        params.put("label", label);
        params.put("max_degree", degree);
        RestResult result = this.client.get(this.path(), params);
        @SuppressWarnings("unchecked")
        Map<String, Double> jaccard = result.readObject(Map.class);
        E.checkState(jaccard.containsKey(JACCARD_SIMILARITY),
                     "The result doesn't have key '%s'", JACCARD_SIMILARITY);
        return jaccard.get(JACCARD_SIMILARITY);
    }

    @SuppressWarnings("unchecked")
    public Map<Object, Double> post(
                               SingleSourceJaccardSimilarityRequest request) {
        this.client.checkApiVersion("0.58", "jaccard similar");
        RestResult result = this.client.post(this.path(), request);
        return result.readObject(Map.class);
    }
}
