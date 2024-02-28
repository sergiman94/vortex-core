
package com.vortex.vortexdb.traversal.algorithm;

import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.structure.VortexEdge;
import com.vortex.vortexdb.traversal.algorithm.records.KneighborRecords;
import com.vortex.vortexdb.traversal.algorithm.steps.EdgeStep;
import com.vortex.vortexdb.type.define.Directions;
import com.vortex.common.util.E;
import org.apache.tinkerpop.gremlin.structure.Edge;

import java.util.Iterator;
import java.util.Set;
import java.util.function.Consumer;

public class KneighborTraverser extends OltpTraverser {

    public KneighborTraverser(Vortex graph) {
        super(graph);
    }

    public Set<Id> kneighbor(Id sourceV, Directions dir,
                             String label, int depth,
                             long degree, long limit) {
        E.checkNotNull(sourceV, "source vertex id");
        this.checkVertexExist(sourceV, "source vertex");
        E.checkNotNull(dir, "direction");
        checkPositive(depth, "k-neighbor max_depth");
        checkDegree(degree);
        checkLimit(limit);

        Id labelId = this.getEdgeLabelId(label);

        Set<Id> latest = newSet();
        Set<Id> all = newSet();

        latest.add(sourceV);

        while (depth-- > 0) {
            long remaining = limit == NO_LIMIT ? NO_LIMIT : limit - all.size();
            latest = this.adjacentVertices(sourceV, latest, dir, labelId,
                                           all, degree, remaining);
            all.addAll(latest);
            if (reachLimit(limit, all.size())) {
                break;
            }
        }

        return all;
    }

    public KneighborRecords customizedKneighbor(Id source, EdgeStep step,
                                                int maxDepth, long limit) {
        E.checkNotNull(source, "source vertex id");
        this.checkVertexExist(source, "source vertex");
        checkPositive(maxDepth, "k-neighbor max_depth");
        checkLimit(limit);

        boolean concurrent = maxDepth >= this.concurrentDepth();

        KneighborRecords records = new KneighborRecords(concurrent,
                                                        source, true);

        Consumer<Id> consumer = v -> {
            if (this.reachLimit(limit, records.size())) {
                return;
            }
            Iterator<Edge> edges = edgesOfVertex(v, step);
            while (!this.reachLimit(limit, records.size()) && edges.hasNext()) {
                Id target = ((VortexEdge) edges.next()).id().otherVertexId();
                records.addPath(v, target);
            }
        };

        while (maxDepth-- > 0) {
            records.startOneLayer(true);
            traverseIds(records.keys(), consumer, concurrent);
            records.finishOneLayer();
        }
        return records;
    }

    private boolean reachLimit(long limit, int size) {
        return limit != NO_LIMIT && size >= limit;
    }
}
