
package com.vortex.vortexdb.traversal.algorithm;

import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.structure.VortexEdge;
import com.vortex.vortexdb.traversal.algorithm.steps.EdgeStep;
import com.vortex.vortexdb.traversal.algorithm.strategy.TraverseStrategy;
import org.apache.tinkerpop.gremlin.structure.Edge;

import java.util.*;
import java.util.function.BiConsumer;

import static com.vortex.vortexdb.traversal.algorithm.VortexTraverser.NO_LIMIT;

public abstract class PathTraverser {

    protected final VortexTraverser traverser;

    protected int stepCount;
    protected final long capacity;
    protected final long limit;
    protected int totalSteps; // TODO: delete or implement abstract method

    protected Map<Id, List<VortexTraverser.Node>> sources;
    protected Map<Id, List<VortexTraverser.Node>> sourcesAll;
    protected Map<Id, List<VortexTraverser.Node>> targets;
    protected Map<Id, List<VortexTraverser.Node>> targetsAll;

    protected Map<Id, List<VortexTraverser.Node>> newVertices;

    protected Set<VortexTraverser.Path> paths;

    protected TraverseStrategy traverseStrategy;

    public PathTraverser(VortexTraverser traverser, TraverseStrategy strategy,
                         Collection<Id> sources, Collection<Id> targets,
                         long capacity, long limit) {
        this.traverser = traverser;
        this.traverseStrategy = strategy;

        this.capacity = capacity;
        this.limit = limit;

        this.stepCount = 0;

        this.sources = this.newMultiValueMap();
        this.sourcesAll = this.newMultiValueMap();
        this.targets = this.newMultiValueMap();
        this.targetsAll = this.newMultiValueMap();

        for (Id id : sources) {
            this.addNode(this.sources, id, new VortexTraverser.Node(id));
        }
        for (Id id : targets) {
            this.addNode(this.targets, id, new VortexTraverser.Node(id));
        }
        this.sourcesAll.putAll(this.sources);
        this.targetsAll.putAll(this.targets);

        this.paths = this.newPathSet();
    }

    public void forward() {
        EdgeStep currentStep = this.nextStep(true);
        if (currentStep == null) {
            return;
        }

        this.beforeTraverse(true);

        // Traversal vertices of previous level
        this.traverseOneLayer(this.sources, currentStep, this::forward);

        this.afterTraverse(currentStep, true);
    }

    public void backward() {
        EdgeStep currentStep = this.nextStep(false);
        if (currentStep == null) {
            return;
        }

        this.beforeTraverse(false);

        currentStep.swithDirection();
        // Traversal vertices of previous level
        this.traverseOneLayer(this.targets, currentStep, this::backward);
        currentStep.swithDirection();

        this.afterTraverse(currentStep, false);
    }

    public abstract EdgeStep nextStep(boolean forward);

    public void beforeTraverse(boolean forward) {
        this.clearNewVertices();
    }

    public void traverseOneLayer(Map<Id, List<VortexTraverser.Node>> vertices,
                                 EdgeStep step,
                                 BiConsumer<Id, EdgeStep> consumer) {
        this.traverseStrategy.traverseOneLayer(vertices, step, consumer);
    }

    public void afterTraverse(EdgeStep step, boolean forward) {
        this.reInitCurrentStepIfNeeded(step, forward);
        this.stepCount++;
    }

    private void forward(Id v, EdgeStep step) {
        this.traverseOne(v, step, true);
    }

    private void backward(Id v, EdgeStep step) {
        this.traverseOne(v, step, false);
    }

    private void traverseOne(Id v, EdgeStep step, boolean forward) {
        if (this.reachLimit()) {
            return;
        }

        Iterator<Edge> edges = this.traverser.edgesOfVertex(v, step);
        while (edges.hasNext()) {
            VortexEdge edge = (VortexEdge) edges.next();
            Id target = edge.id().otherVertexId();

            this.processOne(v, target, forward);
        }
    }

    private void processOne(Id source, Id target, boolean forward) {
        if (forward) {
            this.processOneForForward(source, target);
        } else {
            this.processOneForBackward(source, target);
        }
    }

    protected abstract void processOneForForward(Id source, Id target);

    protected abstract void processOneForBackward(Id source, Id target);

    protected abstract void reInitCurrentStepIfNeeded(EdgeStep step,
                                                      boolean forward);

    public void clearNewVertices() {
        this.newVertices = this.newMultiValueMap();
    }

    public void addNodeToNewVertices(Id id, VortexTraverser.Node node) {
        this.addNode(this.newVertices, id, node);
    }

    public Map<Id, List<VortexTraverser.Node>> newMultiValueMap() {
        return this.traverseStrategy.newMultiValueMap();
    }

    public Set<VortexTraverser.Path> newPathSet() {
        return this.traverseStrategy.newPathSet();
    }

    public void addNode(Map<Id, List<VortexTraverser.Node>> vertices, Id id,
                        VortexTraverser.Node node) {
        this.traverseStrategy.addNode(vertices, id, node);
    }

    public void addNewVerticesToAll(Map<Id, List<VortexTraverser.Node>> targets) {
        this.traverseStrategy.addNewVerticesToAll(this.newVertices, targets);
    }

    public Set<VortexTraverser.Path> paths() {
        return this.paths;
    }

    public int pathCount() {
        return this.paths.size();
    }

    protected boolean finished() {
        return this.stepCount >= this.totalSteps || this.reachLimit();
    }

    protected boolean reachLimit() {
        VortexTraverser.checkCapacity(this.capacity, this.accessedNodes(),
                                    "template paths");
        if (this.limit == NO_LIMIT || this.pathCount() < this.limit) {
            return false;
        }
        return true;
    }

    protected int accessedNodes() {
        int size = 0;
        for (List<VortexTraverser.Node> value : this.sourcesAll.values()) {
            size += value.size();
        }
        for (List<VortexTraverser.Node> value : this.targetsAll.values()) {
            size += value.size();
        }
        return size;
    }
}
