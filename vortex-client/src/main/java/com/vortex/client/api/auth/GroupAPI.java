package com.vortex.client.api.auth;

import com.vortex.client.client.RestClient;
import com.vortex.common.rest.RestResult;
import com.vortex.client.structure.auth.Group;
import com.vortex.client.structure.constant.VortexType;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

public class GroupAPI extends AuthAPI {

    public GroupAPI(RestClient client, String graph) {
        super(client, graph);
    }

    @Override
    protected String type() {
        return VortexType.GROUP.string();
    }

    public Group create(Group group) {
        RestResult result = this.client.post(this.path(), group);
        return result.readObject(Group.class);
    }

    public Group get(Object id) {
        RestResult result = this.client.get(this.path(), formatEntityId(id));
        return result.readObject(Group.class);
    }

    public List<Group> list(int limit) {
        checkLimit(limit, "Limit");
        Map<String, Object> params = ImmutableMap.of("limit", limit);
        RestResult result = this.client.get(this.path(), params);
        return result.readList(this.type(), Group.class);
    }

    public Group update(Group group) {
        String id = formatEntityId(group.id());
        RestResult result = this.client.put(this.path(), id, group);
        return result.readObject(Group.class);
    }

    public void delete(Object id) {
        this.client.delete(this.path(), formatEntityId(id));
    }
}
