package com.vortex.client.api.traverser;

import com.vortex.client.client.RestClient;
import com.vortex.common.rest.RestResult;
import com.vortex.client.structure.traverser.CountRequest;
import com.vortex.common.util.E;

import java.util.Map;

public class CountAPI extends TraversersAPI {

    private static final String COUNT = "count";

    public CountAPI(RestClient client, String graph) {
        super(client, graph);
    }

    @Override
    protected String type() {
        return "count";
    }

    public long post(CountRequest request) {
        this.client.checkApiVersion("0.55", "count");
        RestResult result = this.client.post(this.path(), request);
        @SuppressWarnings("unchecked")
        Map<String, Number> countMap = result.readObject(Map.class);
        E.checkState(countMap.containsKey(COUNT),
                     "The result doesn't have key '%s'", COUNT);
        return countMap.get(COUNT).longValue();
    }
}
