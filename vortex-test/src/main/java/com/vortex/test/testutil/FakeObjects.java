
package com.vortex.test.testutil;

import com.vortex.vortexdb.structure.VortexEdge;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.Map;

public class FakeObjects {

    public static class FakeVertex {

        private String label;
        private Map<String, Object> values;

        public FakeVertex(Object... keyValues) {
            this.label = ElementHelper.getLabelValue(keyValues).get();
            this.values = ElementHelper.asMap(keyValues);

            this.values.remove("label");
        }

        public boolean equalsVertex(Vertex vertex) {
            if (!vertex.label().equals(this.label)) {
                return false;
            }
            for (Map.Entry<String, Object> i : this.values.entrySet()) {
                if (!vertex.property(i.getKey()).value().equals(i.getValue())) {
                    return false;
                }
            }
            return true;
        }
    }

    public static class FakeEdge {

        private String label;
        private Vertex outVertex;
        private Vertex inVertex;
        private Map<String, Object> values;

        public FakeEdge(String label, Vertex outVertex, Vertex inVertex,
                        Object... keyValues) {
            this.label = label;
            this.outVertex = outVertex;
            this.inVertex = inVertex;
            this.values = ElementHelper.asMap(keyValues);
        }

        public boolean equalsEdge(Edge edge) {
            if (!edge.label().equals(this.label) ||
                !((VortexEdge) edge).sourceVertex().equals(this.outVertex) ||
                !((VortexEdge) edge).targetVertex().equals(this.inVertex)) {
                return false;
            }
            for (Map.Entry<String, Object> i : this.values.entrySet()) {
                if (!edge.property(i.getKey()).value().equals(i.getValue())) {
                    return false;
                }
            }
            return true;
        }
    }
}
