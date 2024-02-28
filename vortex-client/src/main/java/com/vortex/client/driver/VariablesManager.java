package com.vortex.client.driver;

import com.vortex.client.api.variables.VariablesAPI;
import com.vortex.client.client.RestClient;

import java.util.Map;

public class VariablesManager {

    private VariablesAPI variablesAPI;

    public VariablesManager(RestClient client, String graph) {
        this.variablesAPI = new VariablesAPI(client, graph);
    }

    public Map<String, Object> get(String key) {
        return this.variablesAPI.get(key);
    }

    public Map<String, Object> set(String key, Object value) {
        return this.variablesAPI.set(key, value);
    }

    public void remove(String key) {
        this.variablesAPI.remove(key);
    }

    public Map<String, Object> all() {
        return this.variablesAPI.all();
    }
}
