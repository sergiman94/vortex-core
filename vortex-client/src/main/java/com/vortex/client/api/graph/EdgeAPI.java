package com.vortex.client.api.graph;

import com.vortex.client.client.RestClient;
import com.vortex.client.exception.NotAllCreatedException;
import com.vortex.common.rest.RestResult;
import com.vortex.client.structure.constant.Direction;
import com.vortex.client.structure.constant.VortexType;
import com.vortex.client.structure.graph.BatchEdgeRequest;
import com.vortex.client.structure.graph.Edge;
import com.vortex.client.structure.graph.Edges;
import com.google.common.collect.ImmutableMap;
import javax.ws.rs.core.MultivaluedHashMap;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class EdgeAPI extends GraphAPI {

    public EdgeAPI(RestClient client, String graph) {
        super(client, graph);
    }

    @Override
    protected String type() {
        return VortexType.EDGE.string();
    }

    public Edge create(Edge edge) {
        RestResult result = this.client.post(this.path(), edge);
        return result.readObject(Edge.class);
    }

    public List<String> create(List<Edge> edges, boolean checkVertex) {
        MultivaluedHashMap<String, Object> headers = new MultivaluedHashMap<>();
        headers.putSingle("Content-Encoding", BATCH_ENCODING);
        Map<String, Object> params = ImmutableMap.of("check_vertex",
                                                     checkVertex);
        RestResult result = this.client.post(this.batchPath(), edges,
                                             headers, params);
        List<String> ids = result.readList(String.class);
        if (edges.size() != ids.size()) {
            throw new NotAllCreatedException(
                      "Not all edges are successfully created, " +
                      "expect '%s', the actual is '%s'",
                      ids, edges.size(), ids.size());
        }
        return ids;
    }

    public List<Edge> update(BatchEdgeRequest request) {
        this.client.checkApiVersion("0.45", "batch property update");
        MultivaluedHashMap<String, Object> headers = new MultivaluedHashMap<>();
        headers.putSingle("Content-Encoding", BATCH_ENCODING);
        RestResult result = this.client.put(this.batchPath(), null,
                                            request, headers);
        return result.readList(this.type(), Edge.class);
    }

    public Edge append(Edge edge) {
        String id = edge.id();
        Map<String, Object> params = ImmutableMap.of("action", "append");
        RestResult result = this.client.put(this.path(), id, edge, params);
        return result.readObject(Edge.class);
    }

    public Edge eliminate(Edge edge) {
        String id = edge.id();
        Map<String, Object> params = ImmutableMap.of("action", "eliminate");
        RestResult result = this.client.put(this.path(), id, edge, params);
        return result.readObject(Edge.class);
    }

    public Edge get(String id) {
        RestResult result = this.client.get(this.path(), id);
        return result.readObject(Edge.class);
    }

    public Edges list(int limit) {
        return this.list(null, null, null, null, 0, null, limit);
    }

    public Edges list(Object vertexId, Direction direction,
                      String label, Map<String, Object> properties,
                      int offset, String page, int limit) {
        return this.list(vertexId, direction, label, properties, false,
                         offset, page, limit);
    }

    public Edges list(Object vertexId, Direction direction, String label,
                      Map<String, Object> properties, boolean keepP,
                      int offset, String page, int limit) {
        checkOffset(offset);
        checkLimit(limit, "Limit");
        String vid = GraphAPI.formatVertexId(vertexId, true);
        String props = GraphAPI.formatProperties(properties);
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("vertex_id", vid);
        params.put("direction", direction);
        params.put("label", label);
        params.put("properties", props);
        params.put("keep_start_p", keepP);
        params.put("offset", offset);
        params.put("limit", limit);
        params.put("page", page);
        RestResult result = this.client.get(this.path(), params);
        return result.readObject(Edges.class);
    }

    public void delete(String id) {
        this.client.delete(this.path(), id);
    }
}
