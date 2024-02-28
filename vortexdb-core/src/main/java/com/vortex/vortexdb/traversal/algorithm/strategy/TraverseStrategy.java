
package com.vortex.vortexdb.traversal.algorithm.strategy;

import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.traversal.algorithm.VortexTraverser;
import com.vortex.vortexdb.traversal.algorithm.steps.EdgeStep;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

public interface TraverseStrategy {

    public abstract void traverseOneLayer(
                         Map<Id, List<VortexTraverser.Node>> vertices,
                         EdgeStep step, BiConsumer<Id, EdgeStep> consumer);

    public abstract Map<Id, List<VortexTraverser.Node>> newMultiValueMap();

    public abstract Set<VortexTraverser.Path> newPathSet();

    public abstract void addNode(Map<Id, List<VortexTraverser.Node>> vertices,
                                 Id id, VortexTraverser.Node node);

    public abstract void addNewVerticesToAll(
                         Map<Id, List<VortexTraverser.Node>> newVertices,
                         Map<Id, List<VortexTraverser.Node>> targets);

    public static TraverseStrategy create(boolean concurrent, Vortex graph) {
        return concurrent ? new ConcurrentTraverseStrategy(graph) :
                            new SingleTraverseStrategy(graph);
    }
}
