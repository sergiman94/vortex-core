
package com.vortex.vortexdb.traversal.algorithm;

import com.vortex.common.util.CollectionUtil;
import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.type.define.Directions;
import com.vortex.common.util.E;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Set;

public class SameNeighborTraverser extends VortexTraverser {

    public SameNeighborTraverser(Vortex graph) {
        super(graph);
    }

    public Set<Id> sameNeighbors(Id vertex, Id other, Directions direction,
                                 String label, long degree, long limit) {
        E.checkNotNull(vertex, "vertex id");
        E.checkNotNull(other, "the other vertex id");
        this.checkVertexExist(vertex, "vertex");
        this.checkVertexExist(other, "other vertex");
        E.checkNotNull(direction, "direction");
        checkDegree(degree);
        checkLimit(limit);

        Id labelId = this.getEdgeLabelId(label);

        Set<Id> sourceNeighbors = IteratorUtils.set(this.adjacentVertices(
                                  vertex, direction, labelId, degree));
        Set<Id> targetNeighbors = IteratorUtils.set(this.adjacentVertices(
                                  other, direction, labelId, degree));
        Set<Id> sameNeighbors = (Set<Id>) CollectionUtil.intersect(
                                sourceNeighbors, targetNeighbors);
        if (limit != NO_LIMIT) {
            int end = Math.min(sameNeighbors.size(), (int) limit);
            sameNeighbors = CollectionUtil.subSet(sameNeighbors, 0, end);
        }
        return sameNeighbors;
    }
}
