package com.vortex.client.api.gremlin;

import com.vortex.client.api.API;
import com.vortex.client.client.RestClient;
import com.vortex.common.rest.RestResult;
import com.vortex.client.structure.constant.VortexType;
import com.vortex.client.structure.gremlin.Response;

public class GremlinAPI extends API {

    public GremlinAPI(RestClient client) {
        super(client);
        this.path(type());
    }

    @Override
    protected String type() {
        return VortexType.GREMLIN.string();
    }

    public Response post(GremlinRequest request) {
        RestResult result = this.client.post(this.path(), request);
        return result.readObject(Response.class);
    }
}
