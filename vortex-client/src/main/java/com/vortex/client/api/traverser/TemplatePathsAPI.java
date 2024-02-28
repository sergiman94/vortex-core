package com.vortex.client.api.traverser;

import com.vortex.client.client.RestClient;
import com.vortex.common.rest.RestResult;
import com.vortex.client.structure.traverser.PathsWithVertices;
import com.vortex.client.structure.traverser.TemplatePathsRequest;

public class TemplatePathsAPI extends TraversersAPI {

    public TemplatePathsAPI(RestClient client, String graph) {
        super(client, graph);
    }

    @Override
    protected String type() {
        return "templatepaths";
    }

    public PathsWithVertices post(TemplatePathsRequest request) {
        this.client.checkApiVersion("0.58", "template paths");
        RestResult result = this.client.post(this.path(), request);
        return result.readObject(PathsWithVertices.class);
    }
}
