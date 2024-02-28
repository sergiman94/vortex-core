package com.vortex.client.api.auth;

import com.vortex.client.client.RestClient;
import com.vortex.common.rest.RestResult;
import com.vortex.client.structure.auth.Belong;
import com.vortex.client.structure.constant.VortexType;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class BelongAPI extends AuthAPI {

    public BelongAPI(RestClient client, String graph) {
        super(client, graph);
    }

    @Override
    protected String type() {
        return VortexType.BELONG.string();
    }

    public Belong create(Belong belong) {
        RestResult result = this.client.post(this.path(), belong);
        return result.readObject(Belong.class);
    }

    public Belong get(Object id) {
        RestResult result = this.client.get(this.path(), formatRelationId(id));
        return result.readObject(Belong.class);
    }

    public List<Belong> list(Object user, Object group, int limit) {
        checkLimit(limit, "Limit");
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("limit", limit);
        params.put("user", formatEntityId(user));
        params.put("group", formatEntityId(group));
        RestResult result = this.client.get(this.path(), params);
        return result.readList(this.type(), Belong.class);
    }

    public Belong update(Belong belong) {
        String id = formatRelationId(belong.id());
        RestResult result = this.client.put(this.path(), id, belong);
        return result.readObject(Belong.class);
    }

    public void delete(Object id) {
        this.client.delete(this.path(), formatRelationId(id));
    }
}
