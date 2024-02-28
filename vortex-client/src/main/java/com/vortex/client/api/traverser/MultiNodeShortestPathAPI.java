package com.vortex.client.api.traverser;

import com.vortex.client.client.RestClient;
import com.vortex.common.rest.RestResult;
import com.vortex.client.structure.traverser.MultiNodeShortestPathRequest;
import com.vortex.client.structure.traverser.PathsWithVertices;

public class MultiNodeShortestPathAPI extends TraversersAPI {

    public MultiNodeShortestPathAPI(RestClient client, String graph) {
        super(client, graph);
    }

    @Override
    protected String type() {
        return "multinodeshortestpath";
    }

    public PathsWithVertices post(MultiNodeShortestPathRequest request) {
        this.client.checkApiVersion("0.58", "multi node shortest path");
        RestResult result = this.client.post(this.path(), request);
        return result.readObject(PathsWithVertices.class);
    }
}
