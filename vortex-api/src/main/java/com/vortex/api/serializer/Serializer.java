
package com.vortex.api.serializer;

import com.vortex.vortexdb.auth.SchemaDefine.AuthElement;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.schema.*;
import com.vortex.vortexdb.traversal.algorithm.CustomizedCrosspointsTraverser.CrosspointsPaths;
import com.vortex.vortexdb.traversal.algorithm.FusiformSimilarityTraverser.SimilarsMap;
import com.vortex.vortexdb.traversal.algorithm.VortexTraverser;
import com.vortex.vortexdb.traversal.algorithm.SingleSourceShortestPathTraverser.NodeWithWeight;
import com.vortex.vortexdb.traversal.algorithm.SingleSourceShortestPathTraverser.WeightedPaths;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public interface Serializer {

    public String writeMap(Map<?, ?> map);

    public String writeList(String label, Collection<?> list);

    public String writePropertyKey(PropertyKey propertyKey);

    public String writePropertyKeys(List<PropertyKey> propertyKeys);

    public String writeVertexLabel(VertexLabel vertexLabel);

    public String writeVertexLabels(List<VertexLabel> vertexLabels);

    public String writeEdgeLabel(EdgeLabel edgeLabel);

    public String writeEdgeLabels(List<EdgeLabel> edgeLabels);

    public String writeIndexlabel(IndexLabel indexLabel);

    public String writeIndexlabels(List<IndexLabel> indexLabels);

    public String writeTaskWithSchema(SchemaElement.TaskWithSchema tws);

    public String writeVertex(Vertex v);

    public String writeVertices(Iterator<Vertex> vertices, boolean paging);

    public String writeEdge(Edge e);

    public String writeEdges(Iterator<Edge> edges, boolean paging);

    public String writeIds(List<Id> ids);

    public String writeAuthElement(AuthElement elem);

    public <V extends AuthElement> String writeAuthElements(String label,
                                                            List<V> users);

    public String writePaths(String name, Collection<VortexTraverser.Path> paths,
                             boolean withCrossPoint, Iterator<Vertex> vertices);

    public default String writePaths(String name,
                                     Collection<VortexTraverser.Path> paths,
                                     boolean withCrossPoint) {
        return this.writePaths(name, paths, withCrossPoint, null);
    }

    public String writeCrosspoints(CrosspointsPaths paths,
                                   Iterator<Vertex> iterator, boolean withPath);

    public String writeSimilars(SimilarsMap similars,
                                Iterator<Vertex> vertices);

    public String writeWeightedPath(NodeWithWeight path,
                                    Iterator<Vertex> vertices);

    public String writeWeightedPaths(WeightedPaths paths,
                                     Iterator<Vertex> vertices);

    public String writeNodesWithPath(String name, List<Id> nodes, long size,
                                     Collection<VortexTraverser.Path> paths,
                                     Iterator<Vertex> vertices);
}
