package com.vortex.client.api.auth;

import com.vortex.client.client.RestClient;
import com.vortex.common.rest.RestResult;
import com.vortex.client.structure.auth.TokenPayload;
import com.vortex.client.structure.constant.VortexType;

public class TokenAPI extends AuthAPI {

    public TokenAPI(RestClient client, String graph) {
        super(client, graph);
    }

    @Override
    protected String type() {
        return VortexType.TOKEN_VERIFY.string();
    }

    public TokenPayload verifyToken() {
        RestResult result = this.client.get(this.path());
        return result.readObject(TokenPayload.class);
    }
}
