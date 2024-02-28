package com.vortex.client.api.graph;

import com.vortex.client.client.RestClient;
import com.vortex.client.exception.InvalidResponseException;
import com.vortex.client.exception.NotAllCreatedException;
import com.vortex.common.rest.RestResult;
import com.vortex.client.structure.constant.VortexType;
import com.vortex.client.structure.graph.BatchOlapPropertyRequest;
import com.vortex.client.structure.graph.BatchVertexRequest;
import com.vortex.client.structure.graph.Vertex;
import com.vortex.client.structure.graph.Vertices;
import com.google.common.collect.ImmutableMap;
import javax.ws.rs.core.MultivaluedHashMap;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class VertexAPI extends GraphAPI {

    public VertexAPI(RestClient client, String graph) {
        super(client, graph);
    }

    @Override
    protected String type() {
        return VortexType.VERTEX.string();
    }

    public Vertex create(Vertex vertex) {
        RestResult result = this.client.post(this.path(), vertex);
        return result.readObject(Vertex.class);
    }

    public List<Object> create(List<Vertex> vertices) {
        MultivaluedHashMap<String, Object> headers = new MultivaluedHashMap<>();
        headers.putSingle("Content-Encoding", BATCH_ENCODING);
        RestResult result = this.client.post(this.batchPath(), vertices,
                                             headers);
        List<Object> ids = result.readList(Object.class);
        if (vertices.size() != ids.size()) {
            throw new NotAllCreatedException(
                      "Not all vertices are successfully created, " +
                      "expect '%s', the actual is '%s'",
                      ids, vertices.size(), ids.size());
        }
        return ids;
    }

    public List<Vertex> update(BatchVertexRequest request) {
        this.client.checkApiVersion("0.45", "batch property update");
        MultivaluedHashMap<String, Object> headers = new MultivaluedHashMap<>();
        headers.putSingle("Content-Encoding", BATCH_ENCODING);
        RestResult result = this.client.put(this.batchPath(), null,
                                            request, headers);
        return result.readList(this.type(), Vertex.class);
    }

    public int update(BatchOlapPropertyRequest request) {
        this.client.checkApiVersion("0.59", "olap property batch update");
        MultivaluedHashMap<String, Object> headers = new MultivaluedHashMap<>();
        headers.putSingle("Content-Encoding", BATCH_ENCODING);
        String path = String.join("/", this.path(), "olap/batch");
        RestResult result = this.client.put(path, null, request, headers);
        Object size = result.readObject(Map.class).get("size");
        if (!(size instanceof Integer)) {
            throw new InvalidResponseException(
                      "The 'size' in response must be int, but got: %s(%s)",
                      size, size.getClass());
        }
        return (int) size;
    }

    public Vertex append(Vertex vertex) {
        String id = GraphAPI.formatVertexId(vertex.id());
        Map<String, Object> params = ImmutableMap.of("action", "append");
        RestResult result = this.client.put(this.path(), id, vertex, params);
        return result.readObject(Vertex.class);
    }

    public Vertex eliminate(Vertex vertex) {
        String id = GraphAPI.formatVertexId(vertex.id());
        Map<String, Object> params = ImmutableMap.of("action", "eliminate");
        RestResult result = this.client.put(this.path(), id, vertex, params);
        return result.readObject(Vertex.class);
    }

    public Vertex get(Object id) {
        String vertexId = GraphAPI.formatVertexId(id);
        RestResult result = this.client.get(this.path(), vertexId);
        return result.readObject(Vertex.class);
    }

    public Vertices list(int limit) {
        return this.list(null, null, 0, null, limit);
    }

    public Vertices list(String label, Map<String, Object> properties,
                         int offset, String page, int limit) {
        return this.list(label, properties, false, offset, page, limit);
    }

    public Vertices list(String label, Map<String, Object> properties,
                         boolean keepP, int offset, String page, int limit) {
        checkOffset(offset);
        checkLimit(limit, "Limit");
        String props = GraphAPI.formatProperties(properties);
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("label", label);
        params.put("properties", props);
        params.put("keep_start_p", keepP);
        params.put("offset", offset);
        params.put("limit", limit);
        params.put("page", page);
        RestResult result = this.client.get(this.path(), params);
        return result.readObject(Vertices.class);
    }

    public void delete(Object id) {
        String vertexId = GraphAPI.formatVertexId(id);
        this.client.delete(this.path(), vertexId);
    }
}
