package com.vortex.client.api.variables;

import com.vortex.client.api.API;
import com.vortex.client.client.RestClient;
import com.vortex.common.rest.RestResult;
import com.vortex.client.structure.constant.VortexType;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class VariablesAPI extends API {

    private static final String PATH = "graphs/%s/%s";

    public VariablesAPI(RestClient client, String graph) {
        super(client);
        this.path(PATH, graph, this.type());
    }

    @Override
    protected String type() {
        return VortexType.VARIABLES.string();
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> get(String key) {
        RestResult result = this.client.get(path(), key);
        return result.readObject(Map.class);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> set(String key, Object value) {
        value = ImmutableMap.of("data", value);
        RestResult result = this.client.put(this.path(), key, value);
        return result.readObject(Map.class);
    }

    public void remove(String key) {
        this.client.delete(path(), key);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> all() {
        RestResult result = this.client.get(path());
        return result.readObject(Map.class);
    }
}
