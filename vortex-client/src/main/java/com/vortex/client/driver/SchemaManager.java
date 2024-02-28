package com.vortex.client.driver;

import com.vortex.client.api.schema.*;
import com.vortex.client.api.task.TaskAPI;
import com.vortex.client.client.RestClient;
import com.vortex.client.structure.SchemaElement;
import com.vortex.client.structure.schema.*;

import java.util.List;
import java.util.Map;

import static com.vortex.client.api.task.TaskAPI.TASK_TIMEOUT;

public class SchemaManager {

    private PropertyKeyAPI propertyKeyAPI;
    private VertexLabelAPI vertexLabelAPI;
    private EdgeLabelAPI edgeLabelAPI;
    private IndexLabelAPI indexLabelAPI;
    private SchemaAPI schemaAPI;
    private TaskAPI taskAPI;

    public SchemaManager(RestClient client, String graph) {
        this.propertyKeyAPI = new PropertyKeyAPI(client, graph);
        this.vertexLabelAPI = new VertexLabelAPI(client, graph);
        this.edgeLabelAPI = new EdgeLabelAPI(client, graph);
        this.indexLabelAPI = new IndexLabelAPI(client, graph);
        this.schemaAPI = new SchemaAPI(client, graph);
        this.taskAPI = new TaskAPI(client, graph);
    }

    public PropertyKey.Builder propertyKey(String name) {
        PropertyKey.Builder builder = new PropertyKey.BuilderImpl(name, this);
        BuilderProxy<PropertyKey.Builder> proxy = new BuilderProxy<>(builder);
        return proxy.proxy();
    }

    public VertexLabel.Builder vertexLabel(String name) {
        VertexLabel.Builder builder = new VertexLabel.BuilderImpl(name, this);
        BuilderProxy<VertexLabel.Builder> proxy = new BuilderProxy<>(builder);
        return proxy.proxy();
    }

    public EdgeLabel.Builder edgeLabel(String name) {
        EdgeLabel.Builder builder = new EdgeLabel.BuilderImpl(name, this);
        BuilderProxy<EdgeLabel.Builder> proxy = new BuilderProxy<>(builder);
        return proxy.proxy();
    }

    public IndexLabel.Builder indexLabel(String name) {
        IndexLabel.Builder builder = new IndexLabel.BuilderImpl(name, this);
        BuilderProxy<IndexLabel.Builder> proxy = new BuilderProxy<>(builder);
        return proxy.proxy();
    }

    public PropertyKey addPropertyKey(PropertyKey propertyKey) {
        return this.addPropertyKey(propertyKey, TASK_TIMEOUT);
    }

    public PropertyKey addPropertyKey(PropertyKey propertyKey, long seconds) {
        PropertyKey.PropertyKeyWithTask task = this.propertyKeyAPI
                                                   .create(propertyKey);
        if (task.taskId() != 0L) {
            this.taskAPI.waitUntilTaskSuccess(task.taskId(), seconds);
        }
        return task.propertyKey();
    }

    public long addPropertyKeyAsync(PropertyKey propertyKey) {
        PropertyKey.PropertyKeyWithTask task = this.propertyKeyAPI
                                                   .create(propertyKey);
        return task.taskId();
    }

    public PropertyKey appendPropertyKey(PropertyKey propertyKey) {
        return this.propertyKeyAPI.append(propertyKey).propertyKey();
    }

    public PropertyKey eliminatePropertyKey(PropertyKey propertyKey) {
        return this.propertyKeyAPI.eliminate(propertyKey).propertyKey();
    }

    public PropertyKey clearPropertyKey(PropertyKey propertyKey) {
        return this.clearPropertyKey(propertyKey, TASK_TIMEOUT);
    }

    public PropertyKey clearPropertyKey(PropertyKey propertyKey, long seconds) {
        PropertyKey.PropertyKeyWithTask task = this.propertyKeyAPI
                                                   .clear(propertyKey);
        if (task.taskId() != 0L) {
            this.taskAPI.waitUntilTaskSuccess(task.taskId(), seconds);
        }
        return task.propertyKey();
    }

    public long clearPropertyKeyAsync(PropertyKey propertyKey) {
        PropertyKey.PropertyKeyWithTask task = this.propertyKeyAPI
                                                   .clear(propertyKey);
        return task.taskId();
    }

    public void removePropertyKey(String name) {
        this.removePropertyKey(name, TASK_TIMEOUT);
    }

    public void removePropertyKey(String name, long seconds) {
        long task = this.propertyKeyAPI.delete(name);
        this.taskAPI.waitUntilTaskSuccess(task, seconds);
    }

    public long removePropertyKeyAsync(String name) {
        return this.propertyKeyAPI.delete(name);
    }

    public PropertyKey getPropertyKey(String name) {
        return this.propertyKeyAPI.get(name);
    }

    public List<PropertyKey> getPropertyKeys() {
        return this.propertyKeyAPI.list();
    }

    public List<PropertyKey> getPropertyKeys(List<String> names) {
        return this.propertyKeyAPI.list(names);
    }

    public VertexLabel addVertexLabel(VertexLabel vertexLabel) {
        return this.vertexLabelAPI.create(vertexLabel);
    }

    public VertexLabel appendVertexLabel(VertexLabel vertexLabel) {
        return this.vertexLabelAPI.append(vertexLabel);
    }

    public VertexLabel eliminateVertexLabel(VertexLabel vertexLabel) {
        return this.vertexLabelAPI.eliminate(vertexLabel);
    }

    public void removeVertexLabel(String name) {
        long task = this.vertexLabelAPI.delete(name);
        this.taskAPI.waitUntilTaskSuccess(task, TASK_TIMEOUT);
    }

    public void removeVertexLabel(String name, long seconds) {
        long task = this.vertexLabelAPI.delete(name);
        this.taskAPI.waitUntilTaskSuccess(task, seconds);
    }

    public long removeVertexLabelAsync(String name) {
        return this.vertexLabelAPI.delete(name);
    }

    public VertexLabel getVertexLabel(String name) {
        return this.vertexLabelAPI.get(name);
    }

    public List<VertexLabel> getVertexLabels() {
        return this.vertexLabelAPI.list();
    }

    public List<VertexLabel> getVertexLabels(List<String> names) {
        return this.vertexLabelAPI.list(names);
    }

    public EdgeLabel addEdgeLabel(EdgeLabel edgeLabel) {
        return this.edgeLabelAPI.create(edgeLabel);
    }

    public EdgeLabel appendEdgeLabel(EdgeLabel edgeLabel) {
        return this.edgeLabelAPI.append(edgeLabel);
    }

    public EdgeLabel eliminateEdgeLabel(EdgeLabel edgeLabel) {
        return this.edgeLabelAPI.eliminate(edgeLabel);
    }

    public void removeEdgeLabel(String name) {
        this.removeEdgeLabel(name, TASK_TIMEOUT);
    }

    public void removeEdgeLabel(String name, long seconds) {
        long task = this.edgeLabelAPI.delete(name);
        this.taskAPI.waitUntilTaskSuccess(task, seconds);
    }

    public long removeEdgeLabelAsync(String name) {
        return this.edgeLabelAPI.delete(name);
    }

    public EdgeLabel getEdgeLabel(String name) {
        return this.edgeLabelAPI.get(name);
    }

    public List<EdgeLabel> getEdgeLabels() {
        return this.edgeLabelAPI.list();
    }

    public List<EdgeLabel> getEdgeLabels(List<String> names) {
        return this.edgeLabelAPI.list(names);
    }

    public IndexLabel addIndexLabel(IndexLabel indexLabel) {
        return this.addIndexLabel(indexLabel, TASK_TIMEOUT);
    }

    public IndexLabel addIndexLabel(IndexLabel indexLabel, long seconds) {
        IndexLabel.IndexLabelWithTask cil = this.indexLabelAPI
                                                .create(indexLabel);
        if (cil.taskId() != 0L) {
            this.taskAPI.waitUntilTaskSuccess(cil.taskId(), seconds);
        }
        return cil.indexLabel();
    }

    public long addIndexLabelAsync(IndexLabel indexLabel) {
        IndexLabel.IndexLabelWithTask cil = this.indexLabelAPI
                                                .create(indexLabel);
        return cil.taskId();
    }

    public IndexLabel appendIndexLabel(IndexLabel indexLabel) {
        return this.indexLabelAPI.append(indexLabel);
    }

    public IndexLabel eliminateIndexLabel(IndexLabel indexLabel) {
        return this.indexLabelAPI.eliminate(indexLabel);
    }

    public void removeIndexLabel(String name) {
        this.removeIndexLabel(name, TASK_TIMEOUT);
    }

    public void removeIndexLabel(String name, long secondss) {
        long task = this.indexLabelAPI.delete(name);
        this.taskAPI.waitUntilTaskSuccess(task, secondss);
    }

    public long removeIndexLabelAsync(String name) {
        return this.indexLabelAPI.delete(name);
    }

    public IndexLabel getIndexLabel(String name) {
        return this.indexLabelAPI.get(name);
    }

    public List<IndexLabel> getIndexLabels() {
        return this.indexLabelAPI.list();
    }

    public List<IndexLabel> getIndexLabels(List<String> names) {
        return this.indexLabelAPI.list(names);
    }

    public Map<String, List<SchemaElement>> getSchema() {
        return this.schemaAPI.list();
    }
}