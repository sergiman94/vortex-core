package com.vortex.client.api.auth;

import com.vortex.client.client.RestClient;
import com.vortex.common.rest.RestResult;
import com.vortex.client.structure.auth.User;
import com.vortex.client.structure.auth.User.UserRole;
import com.vortex.client.structure.constant.VortexType;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

public class UserAPI extends AuthAPI {

    public UserAPI(RestClient client, String graph) {
        super(client, graph);
    }

    @Override
    protected String type() {
        return VortexType.USER.string();
    }

    public User create(User user) {
        RestResult result = this.client.post(this.path(), user);
        return result.readObject(User.class);
    }

    public User get(Object id) {
        RestResult result = this.client.get(this.path(), formatEntityId(id));
        return result.readObject(User.class);
    }

    public UserRole getUserRole(Object id) {
        String idEncoded = RestClient.encode(formatEntityId(id));
        String path = String.join("/", this.path(), idEncoded, "role");
        RestResult result = this.client.get(path);
        return result.readObject(UserRole.class);
    }

    public List<User> list(int limit) {
        checkLimit(limit, "Limit");
        Map<String, Object> params = ImmutableMap.of("limit", limit);
        RestResult result = this.client.get(this.path(), params);
        return result.readList(this.type(), User.class);
    }

    public User update(User user) {
        String id = formatEntityId(user.id());
        RestResult result = this.client.put(this.path(), id, user);
        return result.readObject(User.class);
    }

    public void delete(Object id) {
        this.client.delete(this.path(), formatEntityId(id));
    }
}
