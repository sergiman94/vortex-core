
package com.vortex.vortexdb.traversal.algorithm.steps;

import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.schema.EdgeLabel;
import com.vortex.vortexdb.traversal.algorithm.VortexTraverser;
import com.vortex.vortexdb.traversal.optimize.TraversalUtil;
import com.vortex.vortexdb.type.define.Directions;
import com.vortex.common.util.E;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.tinkerpop.gremlin.structure.Edge;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.vortex.vortexdb.traversal.algorithm.VortexTraverser.DEFAULT_MAX_DEGREE;
import static com.vortex.vortexdb.traversal.algorithm.VortexTraverser.NO_LIMIT;

public class EdgeStep {

    protected Directions direction;
    protected final Map<Id, String> labels;
    protected final Map<Id, Object> properties;
    protected final long degree;
    protected final long skipDegree;

    public EdgeStep(Vortex g, Directions direction) {
        this(g, direction, ImmutableList.of());
    }

    public EdgeStep(Vortex g, List<String> labels) {
        this(g, Directions.BOTH, labels);
    }

    public EdgeStep(Vortex g, Map<String, Object> properties) {
        this(g, Directions.BOTH, ImmutableList.of(), properties);
    }

    public EdgeStep(Vortex g, Directions direction, List<String> labels) {
        this(g, direction, labels, ImmutableMap.of());
    }

    public EdgeStep(Vortex g, Directions direction, List<String> labels,
                    Map<String, Object> properties) {
        this(g, direction, labels, properties,
             Long.parseLong(DEFAULT_MAX_DEGREE), 0L);
    }

    public EdgeStep(Vortex g, Directions direction, List<String> labels,
                    Map<String, Object> properties,
                    long degree, long skipDegree) {
        E.checkArgument(degree == NO_LIMIT || degree > 0L,
                        "The max degree must be > 0 or == -1, but got: %s",
                        degree);
        VortexTraverser.checkSkipDegree(skipDegree, degree,
                                      VortexTraverser.NO_LIMIT);
        this.direction = direction;

        // Parse edge labels
        Map<Id, String> labelIds = new HashMap<>();
        if (labels != null) {
            for (String label : labels) {
                EdgeLabel el = g.edgeLabel(label);
                labelIds.put(el.id(), label);
            }
        }
        this.labels = labelIds;

        // Parse properties
        if (properties == null || properties.isEmpty()) {
            this.properties = null;
        } else {
            this.properties = TraversalUtil.transProperties(g, properties);
        }

        this.degree = degree;
        this.skipDegree = skipDegree;
    }

    public Directions direction() {
        return this.direction;
    }

    public Map<Id, String> labels() {
        return this.labels;
    }

    public Map<Id, Object> properties() {
        return this.properties;
    }

    public long degree() {
        return this.degree;
    }

    public long skipDegree() {
        return this.skipDegree;
    }

    public Id[] edgeLabels() {
        int elsSize = this.labels.size();
        Id[] edgeLabels = this.labels.keySet().toArray(new Id[elsSize]);
        return edgeLabels;
    }

    public void swithDirection() {
        this.direction = this.direction.opposite();
    }

    public long limit() {
        long limit = this.skipDegree > 0L ? this.skipDegree : this.degree;
        return limit;
    }

    @Override
    public String toString() {
        return String.format("EdgeStep{direction=%s,labels=%s,properties=%s}",
                             this.direction, this.labels, this.properties);
    }

    public Iterator<Edge> skipSuperNodeIfNeeded(Iterator<Edge> edges) {
        return VortexTraverser.skipSuperNodeIfNeeded(edges, this.degree,
                                                   this.skipDegree);
    }
}
