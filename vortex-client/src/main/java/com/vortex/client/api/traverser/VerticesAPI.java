package com.vortex.client.api.traverser;

import com.vortex.client.api.graph.GraphAPI;
import com.vortex.client.client.RestClient;
import com.vortex.common.rest.RestResult;
import com.vortex.client.structure.graph.Shard;
import com.vortex.client.structure.graph.Vertex;
import com.vortex.client.structure.graph.Vertices;
import com.vortex.common.util.E;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class VerticesAPI extends TraversersAPI {

    public VerticesAPI(RestClient client, String graph) {
        super(client, graph);
    }

    @Override
    protected String type() {
        return "vertices";
    }

    public List<Vertex> list(List<Object> ids) {
        E.checkArgument(ids != null && !ids.isEmpty(),
                        "Ids can't be null or empty");

        List<String> stringIds = new ArrayList<>(ids.size());
        for (Object id : ids) {
            stringIds.add(GraphAPI.formatVertexId(id, false));
        }

        Map<String, Object> params = new LinkedHashMap<>();
        params.put("ids", stringIds);
        RestResult result = this.client.get(this.path(), params);
        return result.readList(this.type(), Vertex.class);
    }

    public List<Shard> shards(long splitSize) {
        String path = String.join(PATH_SPLITOR, this.path(), "shards");
        Map<String, Object> params = ImmutableMap.of("split_size", splitSize);
        RestResult result = this.client.get(path, params);
        return result.readList("shards", Shard.class);
    }

    public Vertices scan(Shard shard, String page, long pageLimit) {
        E.checkArgument(shard != null, "Shard can't be null");
        String path = String.join(PATH_SPLITOR, this.path(), "scan");
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("start", shard.start());
        params.put("end", shard.end());
        params.put("page", page);
        params.put("page_limit", pageLimit);
        RestResult result = this.client.get(path, params);
        return result.readObject(Vertices.class);
    }
}

