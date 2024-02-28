
package com.vortex.vortexdb.traversal.algorithm;

import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.structure.VortexVertex;
import com.vortex.vortexdb.traversal.algorithm.steps.EdgeStep;
import com.vortex.common.util.E;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

public class MultiNodeShortestPathTraverser extends OltpTraverser {

    public MultiNodeShortestPathTraverser(Vortex graph) {
        super(graph);
    }

    public List<Path> multiNodeShortestPath(Iterator<Vertex> vertices,
                                            EdgeStep step, int maxDepth,
                                            long capacity) {
        List<Vertex> vertexList = IteratorUtils.list(vertices);
        int vertexCount = vertexList.size();
        E.checkState(vertexCount >= 2 && vertexCount <= MAX_VERTICES,
                     "The number of vertices of multiple node shortest path " +
                     "must in [2, %s], but got: %s",
                     MAX_VERTICES, vertexList.size());
        List<Pair<Id, Id>> pairs = newList();
        cmn(vertexList, vertexCount, 2, 0, null, r -> {
            Id source = ((VortexVertex) r.get(0)).id();
            Id target = ((VortexVertex) r.get(1)).id();
            Pair<Id, Id> pair = Pair.of(source, target);
            pairs.add(pair);
        });

        if (maxDepth >= this.concurrentDepth() && vertexCount > 10) {
            return this.multiNodeShortestPathConcurrent(pairs, step,
                                                        maxDepth, capacity);
        } else {
            return this.multiNodeShortestPathSingle(pairs, step,
                                                    maxDepth, capacity);
        }
    }

    public List<Path> multiNodeShortestPathConcurrent(List<Pair<Id, Id>> pairs,
                                                      EdgeStep step,
                                                      int maxDepth,
                                                      long capacity) {
        List<Path> results = new CopyOnWriteArrayList<>();
        ShortestPathTraverser traverser =
                              new ShortestPathTraverser(this.graph());
        this.traversePairs(pairs.iterator(), pair -> {
            Path path = traverser.shortestPath(pair.getLeft(), pair.getRight(),
                                               step, maxDepth, capacity);
            if (!Path.EMPTY.equals(path)) {
                results.add(path);
            }
        });

        return results;
    }

    public List<Path> multiNodeShortestPathSingle(List<Pair<Id, Id>> pairs,
                                                  EdgeStep step, int maxDepth,
                                                  long capacity) {
        List<Path> results = newList();
        ShortestPathTraverser traverser =
                              new ShortestPathTraverser(this.graph());
        for (Pair<Id, Id> pair : pairs) {
            Path path = traverser.shortestPath(pair.getLeft(), pair.getRight(),
                                               step, maxDepth, capacity);
            if (!Path.EMPTY.equals(path)) {
                results.add(path);
            }
        }
        return results;
    }

    private static <T> void cmn(List<T> all, int m, int n, int current,
                                List<T> result, Consumer<List<T>> consumer) {
        assert m <= all.size();
        assert current <= all.size();
        if (result == null) {
            result = newList(n);
        }
        if (n == 0) {
            // All n items are selected
            consumer.accept(result);
            return;
        }
        if (m < n || current >= all.size()) {
            return;
        }

        // Select current item, continue to select C(m-1, n-1)
        int index = result.size();
        result.add(all.get(current));
        cmn(all, m - 1, n - 1, ++current, result, consumer);
        // Not select current item, continue to select C(m-1, n)
        result.remove(index);
        cmn(all, m - 1, n, current, result, consumer);
    }
}
