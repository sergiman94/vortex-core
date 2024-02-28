package com.vortex.client.api.job;

import com.vortex.client.api.API;
import com.vortex.client.client.RestClient;
import com.vortex.client.structure.constant.VortexType;

public abstract class JobAPI extends API {

    // For example: graphs/vortex/jobs/gremlin
    private static final String PATH = "graphs/%s/%s/%s";

    public JobAPI(RestClient client, String graph) {
        super(client);
        this.path(String.format(PATH, graph, this.type(), this.jobType()));
    }

    @Override
    protected String type() {
        return VortexType.JOB.string();
    }

    protected abstract String jobType();
}
