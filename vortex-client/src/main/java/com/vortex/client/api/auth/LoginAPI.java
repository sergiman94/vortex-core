package com.vortex.client.api.auth;

import com.vortex.client.client.RestClient;
import com.vortex.common.rest.RestResult;
import com.vortex.client.structure.auth.Login;
import com.vortex.client.structure.auth.LoginResult;
import com.vortex.client.structure.constant.VortexType;

public class LoginAPI extends AuthAPI {

    public LoginAPI(RestClient client, String graph) {
        super(client, graph);
    }

    @Override
    protected String type() {
        return VortexType.LOGIN.string();
    }

    public LoginResult login(Login login) {
        RestResult result = this.client.post(this.path(), login);
        return result.readObject(LoginResult.class);
    }
}
