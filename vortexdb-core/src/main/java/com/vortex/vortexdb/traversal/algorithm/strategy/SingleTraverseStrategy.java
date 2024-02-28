
package com.vortex.vortexdb.traversal.algorithm.strategy;

import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.traversal.algorithm.VortexTraverser;
import com.vortex.vortexdb.traversal.algorithm.OltpTraverser;
import com.vortex.vortexdb.traversal.algorithm.steps.EdgeStep;

import javax.ws.rs.core.MultivaluedMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

public class SingleTraverseStrategy extends OltpTraverser
                                    implements TraverseStrategy {

    public SingleTraverseStrategy(Vortex graph) {
        super(graph);
    }

    @Override
    public void traverseOneLayer(Map<Id, List<Node>> vertices,
                                 EdgeStep step,
                                 BiConsumer<Id, EdgeStep> biConsumer) {
        for (Id id : vertices.keySet()) {
            biConsumer.accept(id, step);
        }
    }

    @Override
    public Map<Id, List<Node>> newMultiValueMap() {
        return newMultivalueMap();
    }

    @Override
    public Set<Path> newPathSet() {
        return new VortexTraverser.PathSet();
    }

    @Override
    public void addNode(Map<Id, List<Node>> vertices, Id id, Node node) {
        ((MultivaluedMap<Id, Node>) vertices).add(id, node);
    }

    @Override
    public void addNewVerticesToAll(Map<Id, List<Node>> newVertices,
                                    Map<Id, List<Node>> targets) {
        MultivaluedMap<Id, Node> vertices =
                                 (MultivaluedMap<Id, Node>) targets;
        for (Map.Entry<Id, List<Node>> entry : newVertices.entrySet()) {
            vertices.addAll(entry.getKey(), entry.getValue());
        }
    }
}
