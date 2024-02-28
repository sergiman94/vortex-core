
package com.vortex.vortexdb.traversal.algorithm.strategy;

import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.traversal.algorithm.OltpTraverser;
import com.vortex.vortexdb.traversal.algorithm.steps.EdgeStep;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

public class ConcurrentTraverseStrategy extends OltpTraverser
                                        implements TraverseStrategy {

    public ConcurrentTraverseStrategy(Vortex graph) {
        super(graph);
    }

    @Override
    public Map<Id, List<Node>> newMultiValueMap() {
        return new OltpTraverser.ConcurrentMultiValuedMap<>();
    }

    @Override
    public void traverseOneLayer(Map<Id, List<Node>> vertices,
                                 EdgeStep step,
                                 BiConsumer<Id, EdgeStep> biConsumer) {
        traverseIds(vertices.keySet().iterator(), (id) -> {
            biConsumer.accept(id, step);
        });
    }

    @Override
    public Set<Path> newPathSet() {
        return ConcurrentHashMap.newKeySet();
    }

    @Override
    public void addNode(Map<Id, List<Node>> vertices, Id id, Node node) {
        ((ConcurrentMultiValuedMap<Id, Node>) vertices).add(id, node);
    }

    @Override
    public void addNewVerticesToAll(Map<Id, List<Node>> newVertices,
                                    Map<Id, List<Node>> targets) {
        ConcurrentMultiValuedMap<Id, Node> vertices =
                (ConcurrentMultiValuedMap<Id, Node>) targets;
        for (Map.Entry<Id, List<Node>> entry : newVertices.entrySet()) {
            vertices.addAll(entry.getKey(), entry.getValue());
        }
    }
}
