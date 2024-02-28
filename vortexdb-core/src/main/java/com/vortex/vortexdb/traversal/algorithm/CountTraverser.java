
package com.vortex.vortexdb.traversal.algorithm;

import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.query.QueryResults;
import com.vortex.common.iterator.FilterIterator;
import com.vortex.common.iterator.FlatMapperIterator;
import com.vortex.vortexdb.structure.VortexEdge;
import com.vortex.vortexdb.traversal.algorithm.steps.EdgeStep;
import com.vortex.common.util.E;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.tinkerpop.gremlin.structure.Edge;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class CountTraverser extends VortexTraverser {

    private boolean containsTraversed = false;
    private long dedupSize = 1000000L;
    private final Set<Id> dedupSet = newIdSet();
    private final MutableLong count = new MutableLong(0L);

    public CountTraverser(Vortex graph) {
        super(graph);
    }

    public long count(Id source, List<EdgeStep> steps,
                      boolean containsTraversed, long dedupSize) {
        E.checkNotNull(source, "source vertex id");
        this.checkVertexExist(source, "source vertex");
        E.checkArgument(steps != null && !steps.isEmpty(),
                        "The steps can't be empty");
        checkDedupSize(dedupSize);

        this.containsTraversed = containsTraversed;
        this.dedupSize = dedupSize;
        if (this.containsTraversed) {
            this.count.increment();
        }

        int stepNum = steps.size();
        EdgeStep firstStep = steps.get(0);
        if (stepNum == 1) {
            // Just one step, query count and return
            long edgesCount = this.edgesCount(source, firstStep);
            this.count.add(edgesCount);
            return this.count.longValue();
        }

        // Multiple steps, construct first step to iterator
        Iterator<Edge> edges = this.edgesOfVertexWithCount(source, firstStep);
        // Wrap steps to Iterator except last step
        for (int i = 1; i < stepNum - 1; i++) {
            EdgeStep currentStep = steps.get(i);
            edges = new FlatMapperIterator<>(edges, (edge) -> {
                Id target = ((VortexEdge) edge).id().otherVertexId();
                return this.edgesOfVertexWithCount(target, currentStep);
            });
        }

        // The last step, just query count
        EdgeStep lastStep = steps.get(stepNum - 1);
        while (edges.hasNext()) {
            Id target = ((VortexEdge) edges.next()).id().otherVertexId();
            if (this.dedup(target)) {
                continue;
            }
            // Count last layer vertices(without dedup size)
            long edgesCount = this.edgesCount(target, lastStep);
            this.count.add(edgesCount);
        }

        return this.count.longValue();
    }

    private Iterator<Edge> edgesOfVertexWithCount(Id source, EdgeStep step) {
        if (this.dedup(source)) {
            return QueryResults.emptyIterator();
        }
        Iterator<Edge> flatten = this.edgesOfVertex(source, step);
        return new FilterIterator<>(flatten, e -> {
            if (this.containsTraversed) {
                // Count intermediate vertices
                this.count.increment();
            }
            return true;
        });
    }

    private void checkDedupSize(long dedup) {
        checkNonNegativeOrNoLimit(dedup, "dedup size");
    }

    private boolean dedup(Id vertex) {
        if (!this.needDedup()) {
            return false;
        }

        if (this.dedupSet.contains(vertex)) {
            // Skip vertex already traversed
            return true;
        } else if (!this.reachDedup()) {
            // Record vertex not traversed before if not reach dedup size
            this.dedupSet.add(vertex);
        }
        return false;
    }

    private boolean needDedup() {
        return this.dedupSize != 0L;
    }

    private boolean reachDedup() {
        return this.dedupSize != NO_LIMIT &&
               this.dedupSet.size() >= this.dedupSize;
    }
}
