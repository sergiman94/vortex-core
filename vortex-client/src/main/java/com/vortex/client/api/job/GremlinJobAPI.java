package com.vortex.client.api.job;

import com.vortex.client.api.gremlin.GremlinRequest;
import com.vortex.client.api.task.TaskAPI;
import com.vortex.client.client.RestClient;
import com.vortex.common.rest.RestResult;

import java.util.Map;

public class GremlinJobAPI extends JobAPI {

    private static final String JOB_TYPE = "gremlin";

    public GremlinJobAPI(RestClient client, String graph) {
        super(client, graph);
    }

    @Override
    protected String jobType() {
        return JOB_TYPE;
    }

    public long execute(GremlinRequest request) {
        RestResult result = this.client.post(this.path(), request);
        @SuppressWarnings("unchecked")
        Map<String, Object> task = result.readObject(Map.class);
        return TaskAPI.parseTaskId(task);
    }
}
