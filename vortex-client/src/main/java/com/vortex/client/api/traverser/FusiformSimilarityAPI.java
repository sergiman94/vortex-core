package com.vortex.client.api.traverser;

import com.vortex.client.client.RestClient;
import com.vortex.common.rest.RestResult;
import com.vortex.client.structure.traverser.FusiformSimilarity;
import com.vortex.client.structure.traverser.FusiformSimilarityRequest;

public class FusiformSimilarityAPI extends TraversersAPI {

    public FusiformSimilarityAPI(RestClient client, String graph) {
        super(client, graph);
    }

    @Override
    protected String type() {
        return "fusiformsimilarity";
    }

    public FusiformSimilarity post(FusiformSimilarityRequest request) {
        this.client.checkApiVersion("0.49", "fusiform similarity");
        RestResult result = this.client.post(this.path(), request);
        return result.readObject(FusiformSimilarity.class);
    }
}
