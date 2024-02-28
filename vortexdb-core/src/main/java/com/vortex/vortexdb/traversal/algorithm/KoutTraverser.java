
package com.vortex.vortexdb.traversal.algorithm;

import com.vortex.vortexdb.VortexException;
import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.structure.VortexEdge;
import com.vortex.vortexdb.traversal.algorithm.records.KoutRecords;
import com.vortex.vortexdb.traversal.algorithm.steps.EdgeStep;
import com.vortex.vortexdb.type.define.Directions;
import com.vortex.common.util.E;
import org.apache.tinkerpop.gremlin.structure.Edge;

import java.util.Iterator;
import java.util.Set;
import java.util.function.Consumer;

public class KoutTraverser extends OltpTraverser {

    public KoutTraverser(Vortex graph) {
        super(graph);
    }

    public Set<Id> kout(Id sourceV, Directions dir, String label,
                        int depth, boolean nearest,
                        long degree, long capacity, long limit) {
        E.checkNotNull(sourceV, "source vertex id");
        this.checkVertexExist(sourceV, "source vertex");
        E.checkNotNull(dir, "direction");
        checkPositive(depth, "k-out max_depth");
        checkDegree(degree);
        checkCapacity(capacity);
        checkLimit(limit);
        if (capacity != NO_LIMIT) {
            // Capacity must > limit because sourceV is counted in capacity
            E.checkArgument(capacity >= limit && limit != NO_LIMIT,
                            "Capacity can't be less than limit, " +
                            "but got capacity '%s' and limit '%s'",
                            capacity, limit);
        }

        Id labelId = this.getEdgeLabelId(label);

        Set<Id> latest = newIdSet();
        latest.add(sourceV);

        Set<Id> all = newIdSet();
        all.add(sourceV);

        long remaining = capacity == NO_LIMIT ?
                         NO_LIMIT : capacity - latest.size();
        while (depth-- > 0) {
            // Just get limit nodes in last layer if limit < remaining capacity
            if (depth == 0 && limit != NO_LIMIT &&
                (limit < remaining || remaining == NO_LIMIT)) {
                remaining = limit;
            }
            if (nearest) {
                latest = this.adjacentVertices(sourceV, latest, dir, labelId,
                                               all, degree, remaining);
                all.addAll(latest);
            } else {
                latest = this.adjacentVertices(sourceV, latest, dir, labelId,
                                               null, degree, remaining);
            }
            if (capacity != NO_LIMIT) {
                // Update 'remaining' value to record remaining capacity
                remaining -= latest.size();

                if (remaining <= 0 && depth > 0) {
                    throw new VortexException(
                              "Reach capacity '%s' while remaining depth '%s'",
                              capacity, depth);
                }
            }
        }

        return latest;
    }

    public KoutRecords customizedKout(Id source, EdgeStep step,
                                      int maxDepth, boolean nearest,
                                      long capacity, long limit) {
        E.checkNotNull(source, "source vertex id");
        this.checkVertexExist(source, "source vertex");
        checkPositive(maxDepth, "k-out max_depth");
        checkCapacity(capacity);
        checkLimit(limit);
        long[] depth = new long[1];
        depth[0] = maxDepth;
        boolean concurrent = maxDepth >= this.concurrentDepth();

        KoutRecords records = new KoutRecords(concurrent, source, nearest);

        Consumer<Id> consumer = v -> {
            if (this.reachLimit(limit, depth[0], records.size())) {
                return;
            }
            Iterator<Edge> edges = edgesOfVertex(v, step);
            while (!this.reachLimit(limit, depth[0], records.size()) &&
                   edges.hasNext()) {
                Id target = ((VortexEdge) edges.next()).id().otherVertexId();
                records.addPath(v, target);
                this.checkCapacity(capacity, records.accessed(), depth[0]);
            }
        };

        while (depth[0]-- > 0) {
            records.startOneLayer(true);
            this.traverseIds(records.keys(), consumer, concurrent);
            records.finishOneLayer();
        }
        return records;
    }

    private void checkCapacity(long capacity, long accessed, long depth) {
        if (capacity == NO_LIMIT) {
            return;
        }
        if (accessed >= capacity && depth > 0) {
            throw new VortexException(
                      "Reach capacity '%s' while remaining depth '%s'",
                      capacity, depth);
        }
    }

    private boolean reachLimit(long limit, long depth, int size) {
        return limit != NO_LIMIT && depth <= 0 && size >= limit;
    }
}
