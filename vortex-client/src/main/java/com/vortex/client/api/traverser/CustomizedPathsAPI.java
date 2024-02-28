package com.vortex.client.api.traverser;

import com.vortex.client.client.RestClient;
import com.vortex.common.rest.RestResult;
import com.vortex.client.structure.traverser.CustomizedPathsRequest;
import com.vortex.client.structure.traverser.PathsWithVertices;

public class CustomizedPathsAPI extends TraversersAPI {

    public CustomizedPathsAPI(RestClient client, String graph) {
        super(client, graph);
    }

    @Override
    protected String type() {
        return "customizedpaths";
    }

    public PathsWithVertices post(CustomizedPathsRequest request) {
        RestResult result = this.client.post(this.path(), request);
        return result.readObject(PathsWithVertices.class);
    }
}
