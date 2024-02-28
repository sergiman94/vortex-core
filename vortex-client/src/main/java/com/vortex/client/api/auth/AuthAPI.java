package com.vortex.client.api.auth;

import com.vortex.client.api.API;
import com.vortex.client.client.RestClient;
import com.vortex.client.structure.auth.AuthElement;

public abstract class AuthAPI extends API {

    private static final String PATH = "graphs/%s/auth/%s";

    public AuthAPI(RestClient client, String graph) {
        super(client);
        this.path(PATH, graph, this.type());
    }

    public static String formatEntityId(Object id) {
        if (id == null) {
            return null;
        } else if (id instanceof AuthElement) {
            id = ((AuthElement) id).id();
        }
        return String.valueOf(id);
    }

    public static String formatRelationId(Object id) {
        if (id == null) {
            return null;
        } else if (id instanceof AuthElement) {
            id = ((AuthElement) id).id();
        }
        return String.valueOf(id);
    }
}
