package com.vortex.client.driver;

import com.vortex.client.api.metrics.MetricsAPI;
import com.vortex.client.client.RestClient;

import java.util.Map;

public class MetricsManager {

    private MetricsAPI metricsAPI;

    public MetricsManager(RestClient client) {
        this.metricsAPI = new MetricsAPI(client);
    }

    public Map<String, Map<String, Object>> backend() {
        return this.metricsAPI.backend();
    }

    public Map<String, Object> backend(String graph) {
        return this.metricsAPI.backend(graph);
    }

    public Map<String, Map<String, Object>> system() {
        return this.metricsAPI.system();
    }

    /**
     * The nesting level is too deep, may need to optimize the server first
     */
    public Map<String, Map<String, Object>> all() {
        return this.metricsAPI.all();
    }
}
