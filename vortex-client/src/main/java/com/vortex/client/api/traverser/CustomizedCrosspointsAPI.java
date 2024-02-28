package com.vortex.client.api.traverser;

import com.vortex.client.client.RestClient;
import com.vortex.common.rest.RestResult;
import com.vortex.client.structure.traverser.CrosspointsRequest;
import com.vortex.client.structure.traverser.CustomizedCrosspoints;

public class CustomizedCrosspointsAPI extends TraversersAPI {

    public CustomizedCrosspointsAPI(RestClient client, String graph) {
        super(client, graph);
    }

    @Override
    protected String type() {
        return "customizedcrosspoints";
    }

    public CustomizedCrosspoints post(CrosspointsRequest request) {
        RestResult result = this.client.post(this.path(), request);
        return result.readObject(CustomizedCrosspoints.class);
    }
}
