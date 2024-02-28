package com.vortex.client.api.auth;

import com.vortex.client.client.RestClient;
import com.vortex.client.structure.constant.VortexType;
import com.google.common.collect.ImmutableMap;

public class LogoutAPI extends AuthAPI {

    public LogoutAPI(RestClient client, String graph) {
        super(client, graph);
    }

    @Override
    protected String type() {
        return VortexType.LOGOUT.string();
    }

    public void logout() {
        this.client.delete(this.path(), ImmutableMap.of());
    }
}
