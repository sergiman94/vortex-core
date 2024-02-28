package com.vortex.client.driver;

import com.vortex.client.api.gremlin.GremlinAPI;
import com.vortex.client.api.gremlin.GremlinRequest;
import com.vortex.client.api.job.GremlinJobAPI;
import com.vortex.client.client.RestClient;
import com.vortex.client.structure.gremlin.Response;
import com.vortex.client.structure.gremlin.ResultSet;

public class GremlinManager {

    private final GraphManager graphManager;

    private GremlinAPI gremlinAPI;
    private GremlinJobAPI gremlinJobAPI;
    private String graph;

    public GremlinManager(RestClient client, String graph,
                          GraphManager graphManager) {
        this.graphManager = graphManager;
        this.gremlinAPI = new GremlinAPI(client);
        this.gremlinJobAPI = new GremlinJobAPI(client, graph);
        this.graph = graph;
    }

    public ResultSet execute(GremlinRequest request) {
        // Bind "graph" to all graphs
        request.aliases.put("graph", this.graph);
        // Bind "g" to all graphs by custom rule which define in gremlin server.
        request.aliases.put("g", "__g_" + this.graph);

        Response response = this.gremlinAPI.post(request);
        response.graphManager(this.graphManager);
        // TODO: Can add some checks later
        return response.result();
    }

    public long executeAsTask(GremlinRequest request) {
        return this.gremlinJobAPI.execute(request);
    }

    public GremlinRequest.Builder gremlin(String gremlin) {
        return new GremlinRequest.Builder(gremlin, this);
    }
}
