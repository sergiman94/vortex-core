
package com.vortex.vortexdb.traversal.algorithm;

import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.structure.VortexVertex;
import com.vortex.vortexdb.traversal.algorithm.steps.EdgeStep;
import com.vortex.vortexdb.traversal.algorithm.strategy.TraverseStrategy;
import com.vortex.common.util.E;
import com.google.common.collect.ImmutableList;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CollectionPathsTraverser extends VortexTraverser {

    public CollectionPathsTraverser(Vortex graph) {
        super(graph);
    }

    public Collection<Path> paths(Iterator<Vertex> sources,
                                  Iterator<Vertex> targets,
                                  EdgeStep step, int depth, boolean nearest,
                                  long capacity, long limit) {
        checkCapacity(capacity);
        checkLimit(limit);

        List<Id> sourceList = newList();
        while (sources.hasNext()) {
            sourceList.add(((VortexVertex) sources.next()).id());
        }
        int sourceSize = sourceList.size();
        E.checkState(sourceSize >= 1 && sourceSize <= MAX_VERTICES,
                     "The number of source vertices must in [1, %s], " +
                     "but got: %s", MAX_VERTICES, sourceList.size());
        List<Id> targetList = newList();
        while (targets.hasNext()) {
            targetList.add(((VortexVertex) targets.next()).id());
        }
        int targetSize = targetList.size();
        E.checkState(targetSize >= 1 && targetSize <= MAX_VERTICES,
                     "The number of target vertices must in [1, %s], " +
                     "but got: %s", MAX_VERTICES, sourceList.size());
        checkPositive(depth, "max depth");

        TraverseStrategy strategy = TraverseStrategy.create(
                                    depth >= this.concurrentDepth(),
                                    this.graph());
        Traverser traverser;
        if (nearest) {
            traverser = new NearestTraverser(this, strategy,
                                             sourceList, targetList, step,
                                             depth, capacity, limit);
        } else {
            traverser = new Traverser(this, strategy,
                                      sourceList, targetList, step,
                                      depth, capacity, limit);
        }

        do {
            // Forward
            traverser.forward();
            if (traverser.finished()) {
                return traverser.paths();
            }

            // Backward
            traverser.backward();
            if (traverser.finished()) {
                return traverser.paths();
            }
        } while (true);
    }

    private static class Traverser extends PathTraverser {

        protected final EdgeStep step;

        public Traverser(VortexTraverser traverser, TraverseStrategy strategy,
                         Collection<Id> sources, Collection<Id> targets,
                         EdgeStep step, int depth, long capacity, long limit) {
            super(traverser, strategy, sources, targets, capacity, limit);
            this.step = step;
            this.totalSteps = depth;
        }

        @Override
        public EdgeStep nextStep(boolean forward) {
            return this.step;
        }

        @Override
        protected void processOneForForward(Id sourceV, Id targetV) {
            for (Node source : this.sources.get(sourceV)) {
                // If have loop, skip target
                if (source.contains(targetV)) {
                    continue;
                }

                // If cross point exists, path found, concat them
                if (this.targetsAll.containsKey(targetV)) {
                    for (Node target : this.targetsAll.get(targetV)) {
                        List<Id> path = source.joinPath(target);
                        if (!path.isEmpty()) {
                            this.paths.add(new Path(targetV, path));
                            if (this.reachLimit()) {
                                return;
                            }
                        }
                    }
                }

                // Add node to next start-nodes
                this.addNodeToNewVertices(targetV, new Node(targetV, source));
            }
        }

        @Override
        protected void processOneForBackward(Id sourceV, Id targetV) {
            for (Node source : this.targets.get(sourceV)) {
                // If have loop, skip target
                if (source.contains(targetV)) {
                    continue;
                }

                // If cross point exists, path found, concat them
                if (this.sourcesAll.containsKey(targetV)) {
                    for (Node target : this.sourcesAll.get(targetV)) {
                        List<Id> path = source.joinPath(target);
                        if (!path.isEmpty()) {
                            Path newPath = new Path(targetV, path);
                            newPath.reverse();
                            this.paths.add(newPath);
                            if (this.reachLimit()) {
                                return;
                            }
                        }
                    }
                }

                // Add node to next start-nodes
                this.addNodeToNewVertices(targetV, new Node(targetV, source));
            }
        }

        @Override
        protected void reInitCurrentStepIfNeeded(EdgeStep step,
                                                 boolean forward) {
            if (forward) {
                // Re-init sources
                this.sources = this.newVertices;
                // Record all passed vertices
                this.addNewVerticesToAll(this.sourcesAll);
            } else {
                // Re-init targets
                this.targets = this.newVertices;
                // Record all passed vertices
                this.addNewVerticesToAll(this.targetsAll);
            }
        }
    }

    private class NearestTraverser extends Traverser {

        public NearestTraverser(VortexTraverser traverser,
                                TraverseStrategy strategy,
                                Collection<Id> sources, Collection<Id> targets,
                                EdgeStep step, int depth, long capacity,
                                long limit) {
            super(traverser, strategy, sources, targets, step,
                  depth, capacity, limit);
        }

        @Override
        protected void processOneForForward(Id sourceV, Id targetV) {
            Node source = this.sources.get(sourceV).get(0);
            // If have loop, skip target
            if (source.contains(targetV)) {
                return;
            }

            // If cross point exists, path found, concat them
            if (this.targetsAll.containsKey(targetV)) {
                Node node = this.targetsAll.get(targetV).get(0);
                List<Id> path = source.joinPath(node);
                if (!path.isEmpty()) {
                    this.paths.add(new Path(targetV, path));
                    if (this.reachLimit()) {
                        return;
                    }
                }
            }

            // Add node to next start-nodes
            this.addNodeToNewVertices(targetV, new Node(targetV, source));
        }

        @Override
        protected void processOneForBackward(Id sourceV, Id targetV) {
            Node sourcee = this.targets.get(sourceV).get(0);
            // If have loop, skip target
            if (sourcee.contains(targetV)) {
                return;
            }

            // If cross point exists, path found, concat them
            if (this.sourcesAll.containsKey(targetV)) {
                Node node = this.sourcesAll.get(targetV).get(0);
                List<Id> path = sourcee.joinPath(node);
                if (!path.isEmpty()) {
                    Path newPath = new Path(targetV, path);
                    newPath.reverse();
                    this.paths.add(newPath);
                    if (this.reachLimit()) {
                        return;
                    }
                }
            }

            // Add node to next start-nodes
            this.addNodeToNewVertices(targetV, new Node(targetV, sourcee));
        }

        @Override
        protected void reInitCurrentStepIfNeeded(EdgeStep step,
                                                 boolean forward) {
            if (forward) {
                // Re-init targets
                this.sources = this.newVertices;
                // Record all passed vertices
                this.addNewVerticesToAll(this.sourcesAll);
            } else {
                // Re-init targets
                this.targets = this.newVertices;
                // Record all passed vertices
                this.addNewVerticesToAll(this.targetsAll);
            }
        }

        @Override
        public void addNodeToNewVertices(Id id, Node node) {
            this.newVertices.putIfAbsent(id, ImmutableList.of(node));
        }

        @Override
        public void addNewVerticesToAll(Map<Id, List<Node>> targets) {
            for (Map.Entry<Id, List<Node>> entry : this.newVertices.entrySet()) {
                targets.putIfAbsent(entry.getKey(), entry.getValue());
            }
        }

        @Override
        protected int accessedNodes() {
            return this.sourcesAll.size() + this.targetsAll.size();
        }
    }
}
