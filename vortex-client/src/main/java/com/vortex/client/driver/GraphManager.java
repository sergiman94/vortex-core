package com.vortex.client.driver;

import com.vortex.client.annotation.UnimplementedFeature;
import com.vortex.client.api.graph.EdgeAPI;
import com.vortex.client.api.graph.VertexAPI;
import com.vortex.client.client.RestClient;
import com.vortex.client.exception.InvalidOperationException;
import com.vortex.client.structure.GraphElement;
import com.vortex.client.structure.constant.Direction;
import com.vortex.client.structure.constant.T;
import com.vortex.client.structure.graph.*;
import com.vortex.common.util.E;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class GraphManager {

    private final String graph;
    private final VertexAPI vertexAPI;
    private final EdgeAPI edgeAPI;

    public GraphManager(RestClient client, String graph) {
        this.graph = graph;
        this.vertexAPI = new VertexAPI(client, graph);
        this.edgeAPI = new EdgeAPI(client, graph);
    }

    public String graph() {
        return this.graph;
    }

    public Vertex addVertex(Vertex vertex) {
        vertex = this.vertexAPI.create(vertex);
        this.attachManager(vertex);
        return vertex;
    }

    public Vertex addVertex(Object... keyValues) {
        Object label = this.getValue(T.label, keyValues);
        if (!(label instanceof String)) {
            throw new IllegalArgumentException(String.format(
                      "Expect a string value as the vertex label " +
                      "argument, but got: %s", label));
        }
        Vertex vertex = new Vertex(String.valueOf(label));
        //vertex.id(this.getValue(T.id, keyValues));
        this.attachProperties(vertex, keyValues);
        return this.addVertex(vertex);
    }

    public Vertex addVertex(String label, Map<String, Object> properties) {
        return this.addVertex(label, null, properties);
    }

    public Vertex addVertex(String label, Object id,
                            Map<String, Object> properties) {
        Vertex vertex = new Vertex(label);
        vertex.id(id);
        this.attachProperties(vertex, properties);
        return this.addVertex(vertex);
    }

    public Vertex getVertex(Object vertexId) {
        Vertex vertex = this.vertexAPI.get(vertexId);
        this.attachManager(vertex);
        return vertex;
    }

    public List<Vertex> addVertices(List<Vertex> vertices) {
        List<Object> ids = this.vertexAPI.create(vertices);
        for (int i = 0; i < vertices.size(); i++) {
            Vertex vertex = vertices.get(i);
            vertex.id(ids.get(i));
            this.attachManager(vertex);
        }
        return vertices;
    }

    public List<Vertex> listVertices() {
        return this.listVertices(-1);
    }

    public List<Vertex> listVertices(int limit) {
        return this.listVertices(null, null, false, 0, limit);
    }

    public List<Vertex> listVertices(int offset, int limit) {
        return this.listVertices(null, null, false, offset, limit);
    }

    public List<Vertex> listVertices(String label) {
        return this.listVertices(label, null, false, 0, -1);
    }

    public List<Vertex> listVertices(String label, int limit) {
        return this.listVertices(label, null, false, 0, limit);
    }

    public List<Vertex> listVertices(String label,
                                     Map<String, Object> properties) {
        return this.listVertices(label, properties, false, 0, -1);
    }

    public List<Vertex> listVertices(String label,
                                     Map<String, Object> properties,
                                     int limit) {
        return this.listVertices(label, properties, false, 0, limit);
    }

    public List<Vertex> listVertices(String label,
                                     Map<String, Object> properties,
                                     boolean keepP) {
        return this.listVertices(label, properties, keepP, 0, -1);
    }

    public List<Vertex> listVertices(String label,
                                     Map<String, Object> properties,
                                     boolean keepP,
                                     int limit) {
        return this.listVertices(label, properties, keepP, 0, limit);
    }

    public List<Vertex> listVertices(String label,
                                     Map<String, Object> properties,
                                     int offset,
                                     int limit) {
        return this.listVertices(label, properties, false, offset, limit);
    }

    public List<Vertex> listVertices(String label,
                                     Map<String, Object> properties,
                                     boolean keepP,
                                     int offset,
                                     int limit) {
        List<Vertex> vertices = this.vertexAPI.list(label, properties, keepP,
                                                    offset, null, limit)
                                              .results();
        for (Vertex vertex : vertices) {
            this.attachManager(vertex);
        }
        return vertices;
    }

    public Iterator<Vertex> iterateVertices(int sizePerPage) {
        return this.iterateVertices(null, null, sizePerPage);
    }

    public Iterator<Vertex> iterateVertices(String label, int sizePerPage) {
        return this.iterateVertices(label, null, sizePerPage);
    }

    public Iterator<Vertex> iterateVertices(String label,
                                            Map<String, Object> properties,
                                            int sizePerPage) {
        return new GraphIterator<>(this, sizePerPage, (page) -> {
            return this.vertexAPI.list(label, properties, 0, page, sizePerPage);
        });
    }

    public void removeVertex(Object vertexId) {
        this.vertexAPI.delete(vertexId);
    }

    public List<Vertex> updateVertices(BatchVertexRequest request) {
        List<Vertex> newVertices = this.vertexAPI.update(request);
        newVertices.forEach(vertex -> this.attachManager(vertex));
        return newVertices;
    }

    public int updateVertices(BatchOlapPropertyRequest request) {
        return this.vertexAPI.update(request);
    }

    public Vertex appendVertexProperty(Vertex vertex) {
        vertex = this.vertexAPI.append(vertex);
        this.attachManager(vertex);
        return vertex;
    }

    public Vertex eliminateVertexProperty(Vertex vertex) {
        vertex = this.vertexAPI.eliminate(vertex);
        this.attachManager(vertex);
        return vertex;
    }

    public Edge addEdge(Edge edge) {
        if (edge.id() != null) {
            throw new InvalidOperationException(
                      "Not allowed to custom id for edge: '%s'", edge);
        }
        edge = this.edgeAPI.create(edge);
        this.attachManager(edge);
        return edge;
    }

    public Edge addEdge(Vertex source, String label, Vertex target,
                        Object... properties) {
        return this.addEdge(source.id(), label, target.id(), properties);
    }

    public Edge addEdge(Object sourceId, String label, Object targetId,
                        Object... properties) {
        Edge edge = new Edge(label);
        edge.sourceId(sourceId);
        edge.targetId(targetId);
        this.attachProperties(edge, properties);
        return this.addEdge(edge);
    }

    public Edge addEdge(Vertex source, String label, Vertex target,
                        Map<String, Object> properties) {
        return this.addEdge(source.id(), label, target.id(), properties);
    }

    public Edge addEdge(Object sourceId, String label, Object targetId,
                        Map<String, Object> properties) {
        Edge edge = new Edge(label);
        edge.sourceId(sourceId);
        edge.targetId(targetId);
        this.attachProperties(edge, properties);
        return this.addEdge(edge);
    }

    public Edge getEdge(String edgeId) {
        Edge edge = this.edgeAPI.get(edgeId);
        this.attachManager(edge);
        return edge;
    }

    public List<Edge> addEdges(List<Edge> edges) {
        return this.addEdges(edges, true);
    }

    public List<Edge> addEdges(List<Edge> edges, boolean checkVertex) {
        for (Edge edge : edges) {
            edge.sourceId();
            edge.targetId();
        }
        List<String> ids = this.edgeAPI.create(edges, checkVertex);
        for (int i = 0; i < edges.size(); i++) {
            Edge edge = edges.get(i);
            edge.id(ids.get(i));
            this.attachManager(edge);
        }
        return edges;
    }

    public List<Edge> listEdges() {
        return this.listEdges(-1);
    }

    public List<Edge> listEdges(int limit) {
        return this.getEdges(null, null, null, null, 0, limit);
    }

    public List<Edge> listEdges(int offset, int limit) {
        return this.getEdges(null, null, null, null, offset, limit);
    }

    public List<Edge> listEdges(String label) {
        return this.getEdges(null, null, label, null, 0, -1);
    }

    public List<Edge> listEdges(String label, int limit) {
        return this.getEdges(null, null, label, null, 0, limit);
    }

    public List<Edge> listEdges(String label,
                                Map<String, Object> properties) {
        return this.getEdges(null, null, label, properties, 0, -1);
    }

    public List<Edge> listEdges(String label,
                                Map<String, Object> properties,
                                int limit) {
        return this.getEdges(null, null, label, properties, 0, limit);
    }

    public List<Edge> listEdges(String label,
                                Map<String, Object> properties,
                                boolean keepP) {
        return this.getEdges(null, null, label, properties, keepP, 0, -1);
    }

    public List<Edge> listEdges(String label,
                                Map<String, Object> properties,
                                boolean keepP,
                                int limit) {
        return this.getEdges(null, null, label, properties, keepP, 0, limit);
    }

    public List<Edge> getEdges(Object vertexId) {
        return this.getEdges(vertexId, Direction.BOTH, null, null, 0, -1);
    }

    public List<Edge> getEdges(Object vertexId, int limit) {
        return this.getEdges(vertexId, Direction.BOTH, null, null, 0, limit);
    }

    public List<Edge> getEdges(Object vertexId, Direction direction) {
        return this.getEdges(vertexId, direction, null, null, 0, -1);
    }

    public List<Edge> getEdges(Object vertexId,
                               Direction direction,
                               int limit) {
        return this.getEdges(vertexId, direction, null, null, 0, limit);
    }

    public List<Edge> getEdges(Object vertexId,
                               Direction direction,
                               String label) {
        return this.getEdges(vertexId, direction, label, null, 0, -1);
    }

    public List<Edge> getEdges(Object vertexId,
                               Direction direction,
                               String label,
                               int limit) {
        return this.getEdges(vertexId, direction, label, null, 0, limit);
    }

    public List<Edge> getEdges(Object vertexId,
                               Direction direction,
                               String label,
                               Map<String, Object> properties) {
        return this.getEdges(vertexId, direction, label, properties, 0, -1);
    }

    public List<Edge> getEdges(Object vertexId,
                               Direction direction,
                               String label,
                               Map<String, Object> properties,
                               int offset,
                               int limit) {
        return this.getEdges(vertexId, direction, label, properties, false,
                             offset, limit);
    }

    public List<Edge> getEdges(Object vertexId,
                               Direction direction,
                               String label,
                               Map<String, Object> properties,
                               boolean keepP,
                               int offset,
                               int limit) {
        List<Edge> edges = this.edgeAPI.list(vertexId, direction, label,
                                             properties, keepP,
                                             offset, null, limit)
                                       .results();
        for (Edge edge : edges) {
            this.attachManager(edge);
        }
        return edges;
    }

    public Iterator<Edge> iterateEdges(int sizePerPage) {
        return this.iterateEdges(null, (Map<String, Object>) null, sizePerPage);
    }

    @UnimplementedFeature(desc = "Server doesn't support paging by label")
    public Iterator<Edge> iterateEdges(String label, int sizePerPage) {
        return this.iterateEdges(label, (Map<String, Object>) null, sizePerPage);
    }

    @UnimplementedFeature(desc = "Server doesn't support paging by label and properties")
    public Iterator<Edge> iterateEdges(String label,
                                       Map<String, Object> properties,
                                       int sizePerPage) {
        return new GraphIterator<>(this, sizePerPage, (page) -> {
            return this.edgeAPI.list(null, null, label, properties,
                                     0, page, sizePerPage);
        });
    }

    public Iterator<Edge> iterateEdges(Object vertexId, int sizePerPage) {
        return this.iterateEdges(vertexId, Direction.BOTH, null, null,
                                 sizePerPage);
    }

    public Iterator<Edge> iterateEdges(Object vertexId,
                                       Direction direction,
                                       int sizePerPage) {
        return this.iterateEdges(vertexId, direction, null, null, sizePerPage);
    }

    public Iterator<Edge> iterateEdges(Object vertexId,
                                       Direction direction,
                                       String label,
                                       int sizePerPage) {
        return this.iterateEdges(vertexId, direction, label, null, sizePerPage);
    }

    public Iterator<Edge> iterateEdges(Object vertexId,
                                       Direction direction,
                                       String label,
                                       Map<String, Object> properties,
                                       int sizePerPage) {
        return new GraphIterator<>(this, sizePerPage, (page) -> {
            return this.edgeAPI.list(vertexId, direction, label, properties,
                                     0, page, sizePerPage);
        });
    }

    public void removeEdge(String edgeId) {
        this.edgeAPI.delete(edgeId);
    }

    public List<Edge> updateEdges(BatchEdgeRequest request) {
        List<Edge> newEdges = this.edgeAPI.update(request);
        newEdges.forEach(edge -> this.attachManager(edge));
        return newEdges;
    }

    public Edge appendEdgeProperty(Edge edge) {
        edge = this.edgeAPI.append(edge);
        this.attachManager(edge);
        return edge;
    }

    public Edge eliminateEdgeProperty(Edge edge) {
        edge = this.edgeAPI.eliminate(edge);
        this.attachManager(edge);
        return edge;
    }

    private Object getValue(String key, Object... keyValues) {
        E.checkArgument((keyValues.length & 0x01) == 0,
                        "The number of parameters must be even");
        Object value = null;
        for (int i = 0; i < keyValues.length; i = i + 2) {
            if (keyValues[i].equals(key)) {
                value = keyValues[i + 1];
                break;
            }
        }
        return value;
    }

    private void attachProperties(GraphElement element, Object... properties) {
        E.checkArgument((properties.length & 0x01) == 0,
                        "The number of properties must be even");
        for (int i = 0; i < properties.length; i = i + 2) {
            if (!properties[i].equals(T.id) &&
                !properties[i].equals(T.label)) {
                element.property((String) properties[i], properties[i + 1]);
            }
        }
    }

    private void attachProperties(GraphElement element,
                                  Map<String, Object> properties) {
        if (properties != null) {
            for (Map.Entry<String, Object> entry : properties.entrySet()) {
                element.property(entry.getKey(), entry.getValue());
            }
        }
    }

    private void attachManager(GraphElement element) {
        element.attachManager(this);
    }
}
