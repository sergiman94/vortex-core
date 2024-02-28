package com.vortex.client.structure.graph;

import com.vortex.client.driver.GraphManager;
import com.vortex.common.util.Log;
import org.slf4j.Logger;

import java.util.*;

/**
 * Vortex is a mirror of server-side data(vertex/edge), it used to speed up
 * data access. Note, however, the memory can't hold large amounts of data.
 */
public class Graph {

    private static final Logger LOG = Log.logger(Graph.class);

    private Map<Object, VortexVertex> vortexVerticesMap;
    private List<VortexEdge> vortexEdges;

    public Graph(GraphManager graph) {
        LOG.debug("Loading Graph...");

        List<Vertex> vertices = graph.listVertices();
        LOG.debug("Loaded vertices: {}", vertices.size());

        List<Edge> edges = graph.listEdges();
        LOG.debug("Loaded edges: {}", edges.size());

        this.mergeEdges2Vertices(vertices, edges);
        LOG.debug("Loaded Graph");
    }

    public Graph(List<Vertex> vertices, List<Edge> edges) {
        this.mergeEdges2Vertices(vertices, edges);
    }

    public Iterator<VortexVertex> vertices() {
        return this.vortexVerticesMap.values().iterator();
    }

    public VortexVertex vertex(Object id) {
        return this.vortexVerticesMap.get(id);
    }

    public Iterator<VortexEdge> edges() {
        return this.vortexEdges.iterator();
    }

    private void mergeEdges2Vertices(List<Vertex> vertices,
                                     List<Edge> edges) {
        this.vortexVerticesMap = new HashMap<>(vertices.size());
        for (Vertex v : vertices) {
            this.vortexVerticesMap.put(v.id(), new VortexVertex(v));
        }

        this.vortexEdges = new ArrayList<>(edges.size());
        for (Edge e : edges) {
            VortexVertex src = this.vortexVerticesMap.get(e.sourceId());
            VortexVertex tgt = this.vortexVerticesMap.get(e.targetId());

            VortexEdge edge = new VortexEdge(e);
            edge.source(src);
            edge.target(tgt);

            src.addEdge(edge);
            tgt.addEdge(edge);

            this.vortexEdges.add(edge);
        }
    }

    public static class VortexVertex {

        private Vertex vertex;
        private List<VortexEdge> edges;

        public VortexVertex(Vertex v) {
            this.vertex = v;
            this.edges = new ArrayList<>();
        }

        public Vertex vertex() {
            return this.vertex;
        }

        public void vertex(Vertex vertex) {
            this.vertex = vertex;
        }

        public void addEdge(VortexEdge e) {
            this.edges.add(e);
        }

        public List<VortexEdge> getEdges() {
            return this.edges;
        }

        @Override
        public String toString() {
            return String.format("VortexVertex{vertex=%s, edges=%s}",
                                 this.vertex, this.edges);
        }
    }

    public static class VortexEdge {

        private Edge edge;
        private VortexVertex source;
        private VortexVertex target;

        public VortexEdge(Edge e) {
            this.edge = e;
        }

        public Edge edge() {
            return this.edge;
        }

        public VortexVertex source() {
            return this.source;
        }

        public void source(VortexVertex source) {
            this.source = source;
        }

        public VortexVertex target() {
            return this.target;
        }

        public void target(VortexVertex target) {
            this.target = target;
        }

        public VortexVertex other(VortexVertex vertex) {
            return vertex == this.source ? this.target : this.source;
        }

        @Override
        public String toString() {
            return String.format("VortexEdge{edge=%s, sourceId=%s, targetId=%s}",
                                 this.edge,
                                 this.source.vertex(),
                                 this.target.vertex());
        }
    }
}
