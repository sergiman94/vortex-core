
package com.vortex.vortexdb.auth;

import com.vortex.vortexdb.backend.id.Id;

public class UserWithRole {

    private final Id userId;
    private final String username;
    private final RolePermission role;

    public UserWithRole(String username) {
        this(null, username, null);
    }

    public UserWithRole(Id userId, String username, RolePermission role) {
        this.userId = userId;
        this.username = username;
        this.role = role;
    }

    public Id userId() {
        return this.userId;
    }

    public String username() {
        return this.username;
    }

    public RolePermission role() {
        return this.role;
    }
}
