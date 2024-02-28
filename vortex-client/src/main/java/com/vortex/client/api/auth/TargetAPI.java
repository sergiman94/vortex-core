package com.vortex.client.api.auth;

import com.vortex.client.client.RestClient;
import com.vortex.common.rest.RestResult;
import com.vortex.client.structure.auth.Target;
import com.vortex.client.structure.constant.VortexType;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

public class TargetAPI extends AuthAPI {

    public TargetAPI(RestClient client, String graph) {
        super(client, graph);
    }

    @Override
    protected String type() {
        return VortexType.TARGET.string();
    }

    public Target create(Target target) {
        RestResult result = this.client.post(this.path(), target);
        return result.readObject(Target.class);
    }

    public Target get(Object id) {
        RestResult result = this.client.get(this.path(), formatEntityId(id));
        return result.readObject(Target.class);
    }

    public List<Target> list(int limit) {
        checkLimit(limit, "Limit");
        Map<String, Object> params = ImmutableMap.of("limit", limit);
        RestResult result = this.client.get(this.path(), params);
        return result.readList(this.type(), Target.class);
    }

    public Target update(Target target) {
        String id = formatEntityId(target.id());
        RestResult result = this.client.put(this.path(), id, target);
        return result.readObject(Target.class);
    }

    public void delete(Object id) {
        this.client.delete(this.path(), formatEntityId(id));
    }
}
