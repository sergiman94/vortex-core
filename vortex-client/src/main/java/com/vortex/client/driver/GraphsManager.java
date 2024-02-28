package com.vortex.client.driver;

import com.vortex.client.api.graphs.GraphsAPI;
import com.vortex.client.client.RestClient;
import com.vortex.client.structure.constant.GraphMode;
import com.vortex.client.structure.constant.GraphReadMode;

import java.util.List;
import java.util.Map;

public class GraphsManager {

    private GraphsAPI graphsAPI;

    public GraphsManager(RestClient client) {
        this.graphsAPI = new GraphsAPI(client);
    }

    public Map<String, String> createGraph(String name, String configText) {
        return this.graphsAPI.create(name, null, configText);
    }

    public Map<String, String> cloneGraph(String name, String cloneGraphName) {
        return this.graphsAPI.create(name, cloneGraphName, null);
    }

    public Map<String, String> cloneGraph(String name, String cloneGraphName,
                                          String configText) {
        return this.graphsAPI.create(name, cloneGraphName, configText);
    }

    public Map<String, String> getGraph(String graph) {
        return this.graphsAPI.get(graph);
    }

    public List<String> listGraph() {
        return this.graphsAPI.list();
    }

    public void clearGraph(String graph, String message) {
        this.graphsAPI.clear(graph, message);
    }

    public void dropGraph(String graph, String message) {
        this.graphsAPI.drop(graph, message);
    }

    public void mode(String graph, GraphMode mode) {
        this.graphsAPI.mode(graph, mode);
    }

    public GraphMode mode(String graph) {
        return this.graphsAPI.mode(graph);
    }

    public void readMode(String graph, GraphReadMode readMode) {
        this.graphsAPI.readMode(graph, readMode);
    }

    public GraphReadMode readMode(String graph) {
        return this.graphsAPI.readMode(graph);
    }
}
