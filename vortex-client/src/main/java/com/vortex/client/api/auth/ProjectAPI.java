package com.vortex.client.api.auth;

import com.vortex.client.client.RestClient;
import com.vortex.common.rest.RestResult;
import com.vortex.client.structure.auth.Project;
import com.vortex.client.structure.constant.VortexType;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class ProjectAPI extends AuthAPI {

    private static final String ACTION_ADD_GRAPH = "add_graph";
    private static final String ACTION_REMOVE_GRAPH = "remove_graph";

    public ProjectAPI(RestClient client, String graph) {
        super(client, graph);
    }

    @Override
    protected String type() {
        return VortexType.PROJECT.string();
    }

    public Project create(Project project) {
        RestResult result = this.client.post(this.path(), project);
        return result.readObject(Project.class);
    }

    public Project get(Object id) {
        RestResult result = this.client.get(this.path(), formatEntityId(id));
        return result.readObject(Project.class);
    }

    public List<Project> list(long limit) {
        checkLimit(limit, "Limit");
        Map<String, Object> params = ImmutableMap.of("limit", limit);
        RestResult result = this.client.get(this.path(), params);
        return result.readList(this.type(), Project.class);
    }

    public Project update(Project project) {
        String id = formatEntityId(project.id());
        RestResult result = this.client.put(this.path(), id, project);
        return result.readObject(Project.class);
    }

    public void delete(Object id) {
        this.client.delete(this.path(), formatEntityId(id));
    }

    public Project addGraphs(Object projectId, Set<String> graphs) {
        Project project = new Project();
        project.graphs(graphs);
        RestResult result = this.client.put(this.path(),
                                            formatEntityId(projectId),
                                            project,
                                            ImmutableMap.of("action",
                                                            ACTION_ADD_GRAPH));
        return result.readObject(Project.class);
    }

    public Project removeGraphs(Object projectId, Set<String> graphs) {
        Project project = new Project();
        project.graphs(graphs);
        RestResult result = this.client.put(this.path(),
                                            formatEntityId(projectId),
                                            project,
                                            ImmutableMap.of("action",
                                                            ACTION_REMOVE_GRAPH));
        return result.readObject(Project.class);
    }
}
