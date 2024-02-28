package com.vortex.client.api.version;

import com.vortex.client.api.API;
import com.vortex.client.client.RestClient;
import com.vortex.common.rest.RestResult;
import com.vortex.client.structure.constant.VortexType;
import com.vortex.client.structure.version.Versions;

public class VersionAPI extends API {

    public VersionAPI(RestClient client) {
        super(client);
        this.path(this.type());
    }

    @Override
    protected String type() {
        return VortexType.VERSION.string();
    }

    public Versions get() {
        RestResult result = this.client.get(this.path());
        return result.readObject(Versions.class);
    }
}
