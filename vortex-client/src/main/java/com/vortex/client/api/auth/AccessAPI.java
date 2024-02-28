package com.vortex.client.api.auth;

import com.vortex.client.client.RestClient;
import com.vortex.common.rest.RestResult;
import com.vortex.client.structure.auth.Access;
import com.vortex.client.structure.constant.VortexType;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class AccessAPI extends AuthAPI {

    public AccessAPI(RestClient client, String graph) {
        super(client, graph);
    }

    @Override
    protected String type() {
        return VortexType.ACCESS.string();
    }

    public Access create(Access access) {
        RestResult result = this.client.post(this.path(), access);
        return result.readObject(Access.class);
    }

    public Access get(Object id) {
        RestResult result = this.client.get(this.path(), formatRelationId(id));
        return result.readObject(Access.class);
    }

    public List<Access> list(Object group, Object target, int limit) {
        checkLimit(limit, "Limit");
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("limit", limit);
        params.put("group", formatEntityId(group));
        params.put("target", formatEntityId(target));
        RestResult result = this.client.get(this.path(), params);
        return result.readList(this.type(), Access.class);
    }

    public Access update(Access access) {
        String id = formatRelationId(access.id());
        RestResult result = this.client.put(this.path(), id, access);
        return result.readObject(Access.class);
    }

    public void delete(Object id) {
        this.client.delete(this.path(), formatRelationId(id));
    }
}
