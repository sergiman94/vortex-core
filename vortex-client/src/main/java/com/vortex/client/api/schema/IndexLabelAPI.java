package com.vortex.client.api.schema;

import com.vortex.client.api.task.TaskAPI;
import com.vortex.client.client.RestClient;
import com.vortex.client.exception.NotSupportException;
import com.vortex.common.rest.RestResult;
import com.vortex.client.structure.SchemaElement;
import com.vortex.client.structure.constant.VortexType;
import com.vortex.client.structure.constant.IndexType;
import com.vortex.client.structure.schema.IndexLabel;
import com.vortex.common.util.E;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

public class IndexLabelAPI extends SchemaElementAPI {

    public IndexLabelAPI(RestClient client, String graph) {
        super(client, graph);
    }

    @Override
    protected String type() {
        return VortexType.INDEX_LABEL.string();
    }

    public IndexLabel.IndexLabelWithTask create(IndexLabel indexLabel) {
        Object il = this.checkCreateOrUpdate(indexLabel);
        RestResult result = this.client.post(this.path(), il);
        return result.readObject(IndexLabel.IndexLabelWithTask.class);
    }

    public IndexLabel append(IndexLabel indexLabel) {
        if (this.client.apiVersionLt("0.50")) {
            throw new NotSupportException("action append on index label");
        }

        String id = indexLabel.name();
        Map<String, Object> params = ImmutableMap.of("action", "append");
        Object il = this.checkCreateOrUpdate(indexLabel);
        RestResult result = this.client.put(this.path(), id, il, params);
        return result.readObject(IndexLabel.class);
    }

    public IndexLabel eliminate(IndexLabel indexLabel) {
        if (this.client.apiVersionLt("0.50")) {
            throw new NotSupportException("action eliminate on index label");
        }

        String id = indexLabel.name();
        Map<String, Object> params = ImmutableMap.of("action", "eliminate");
        Object il = this.checkCreateOrUpdate(indexLabel);
        RestResult result = this.client.put(this.path(), id, il, params);
        return result.readObject(IndexLabel.class);
    }

    public IndexLabel get(String name) {
        RestResult result = this.client.get(this.path(), name);
        return result.readObject(IndexLabel.class);
    }

    public List<IndexLabel> list() {
        RestResult result = this.client.get(this.path());
        return result.readList(this.type(), IndexLabel.class);
    }

    public List<IndexLabel> list(List<String> names) {
        this.client.checkApiVersion("0.48", "getting schema by names");
        E.checkArgument(names != null && !names.isEmpty(),
                        "The index label names can't be null or empty");
        Map<String, Object> params = ImmutableMap.of("names", names);
        RestResult result = this.client.get(this.path(), params);
        return result.readList(this.type(), IndexLabel.class);
    }

    public long delete(String name) {
        RestResult result = this.client.delete(this.path(), name);
        @SuppressWarnings("unchecked")
        Map<String, Object> task = result.readObject(Map.class);
        return TaskAPI.parseTaskId(task);
    }

    @Override
    protected Object checkCreateOrUpdate(SchemaElement schemaElement) {
        IndexLabel indexLabel = (IndexLabel) schemaElement;
        if (indexLabel.indexType() == IndexType.SHARD) {
            this.client.checkApiVersion("0.43", "shard index");
        } else if (indexLabel.indexType() == IndexType.UNIQUE) {
            this.client.checkApiVersion("0.44", "unique index");
        }

        IndexLabel il = indexLabel;
        if (this.client.apiVersionLt("0.50")) {
            E.checkArgument(indexLabel.userdata() == null ||
                            indexLabel.userdata().isEmpty(),
                            "Not support userdata of index label until api " +
                            "version 0.50");
            E.checkArgument(indexLabel.rebuild(),
                            "Not support rebuild of index label until api " +
                            "version 0.57");
            il = indexLabel.switchV49();
        } else if (this.client.apiVersionLt("0.57")) {
            E.checkArgument(indexLabel.rebuild(),
                            "Not support rebuild of index label until api " +
                            "version 0.57");
            il = indexLabel.switchV56();
        }
        return il;
    }
}
