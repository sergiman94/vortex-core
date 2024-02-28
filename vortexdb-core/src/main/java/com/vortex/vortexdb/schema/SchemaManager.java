
package com.vortex.vortexdb.schema;

import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.backend.transaction.SchemaTransaction;
import com.vortex.vortexdb.exception.NotFoundException;
import com.vortex.vortexdb.schema.builder.EdgeLabelBuilder;
import com.vortex.vortexdb.schema.builder.IndexLabelBuilder;
import com.vortex.vortexdb.schema.builder.PropertyKeyBuilder;
import com.vortex.vortexdb.schema.builder.VertexLabelBuilder;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.common.util.E;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.List;
import java.util.stream.Collectors;

public class SchemaManager {

    private final SchemaTransaction transaction;
    private Vortex graph;

    public SchemaManager(SchemaTransaction transaction, Vortex graph) {
        E.checkNotNull(transaction, "transaction");
        E.checkNotNull(graph, "graph");
        this.transaction = transaction;
        this.graph = graph;
    }

    public Vortex proxy(Vortex graph) {
        E.checkNotNull(graph, "graph");
        Vortex old = this.graph;
        this.graph = graph;
        return old;
    }

    public PropertyKey.Builder propertyKey(String name) {
        return new PropertyKeyBuilder(this.transaction, this.graph, name);
    }

    public VertexLabel.Builder vertexLabel(String name) {
        return new VertexLabelBuilder(this.transaction, this.graph, name);
    }

    public EdgeLabel.Builder edgeLabel(String name) {
        return new EdgeLabelBuilder(this.transaction, this.graph, name);
    }

    public IndexLabel.Builder indexLabel(String name) {
        return new IndexLabelBuilder(this.transaction, this.graph, name);
    }

    public PropertyKey getPropertyKey(String name) {
        E.checkArgumentNotNull(name, "The name parameter can't be null");
        PropertyKey propertyKey = this.transaction.getPropertyKey(name);
        checkExists(VortexType.PROPERTY_KEY, propertyKey, name);
        return propertyKey;
    }

    public VertexLabel getVertexLabel(String name) {
        E.checkArgumentNotNull(name, "The name parameter can't be null");
        VertexLabel vertexLabel = this.transaction.getVertexLabel(name);
        checkExists(VortexType.VERTEX_LABEL, vertexLabel, name);
        return vertexLabel;
    }

    public EdgeLabel getEdgeLabel(String name) {
        E.checkArgumentNotNull(name, "The name parameter can't be null");
        EdgeLabel edgeLabel = this.transaction.getEdgeLabel(name);
        checkExists(VortexType.EDGE_LABEL, edgeLabel, name);
        return edgeLabel;
    }

    public IndexLabel getIndexLabel(String name) {
        E.checkArgumentNotNull(name, "The name parameter can't be null");
        IndexLabel indexLabel = this.transaction.getIndexLabel(name);
        checkExists(VortexType.INDEX_LABEL, indexLabel, name);
        return indexLabel;
    }

    public List<PropertyKey> getPropertyKeys() {
        return this.graph.propertyKeys().stream()
                   .filter(pk -> !Graph.Hidden.isHidden(pk.name()))
                   .collect(Collectors.toList());
    }

    public List<VertexLabel> getVertexLabels() {
        return this.graph.vertexLabels().stream()
                   .filter(vl -> !Graph.Hidden.isHidden(vl.name()))
                   .collect(Collectors.toList());
    }

    public List<EdgeLabel> getEdgeLabels() {
        return this.graph.edgeLabels().stream()
                   .filter(el -> !Graph.Hidden.isHidden(el.name()))
                   .collect(Collectors.toList());
    }

    public List<IndexLabel> getIndexLabels() {
        return this.graph.indexLabels().stream()
                   .filter(il -> !Graph.Hidden.isHidden(il.name()))
                   .collect(Collectors.toList());
    }

    public void copyFrom(SchemaManager schema) {
        for (PropertyKey pk : schema.getPropertyKeys()) {
            new PropertyKeyBuilder(this.transaction, this.graph, pk).create();
        }
        for (VertexLabel vl : schema.getVertexLabels()) {
            new VertexLabelBuilder(this.transaction, this.graph, vl).create();
        }
        for (EdgeLabel el : schema.getEdgeLabels()) {
            new EdgeLabelBuilder(this.transaction, this.graph, el).create();
        }
        for (IndexLabel il : schema.getIndexLabels()) {
            new IndexLabelBuilder(this.transaction, this.graph, il).create();
        }
    }

    private static void checkExists(VortexType type, Object object, String name) {
        if (object == null) {
            throw new NotFoundException("%s with name '%s' does not exist",
                                        type.readableName(), name);
        }
    }
}
