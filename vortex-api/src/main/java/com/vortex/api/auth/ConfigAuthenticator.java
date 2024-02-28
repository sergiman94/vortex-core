
package com.vortex.api.auth;

import com.vortex.vortexdb.auth.AuthManager;
import com.vortex.vortexdb.auth.RolePermission;
import com.vortex.vortexdb.auth.UserWithRole;
import com.vortex.vortexdb.backend.id.IdGenerator;
import com.vortex.common.config.VortexConfig;
import com.vortex.api.config.ServerOptions;
import com.vortex.common.util.E;
import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraphTokens;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ConfigAuthenticator implements VortexAuthenticator {

    public static final String KEY_USERNAME =
                               CredentialGraphTokens.PROPERTY_USERNAME;
    public static final String KEY_PASSWORD =
                               CredentialGraphTokens.PROPERTY_PASSWORD;

    private final Map<String, String> tokens;

    public ConfigAuthenticator() {
        this.tokens = new HashMap<>();
    }

    @Override
    public void setup(VortexConfig config) {
        this.tokens.putAll(config.getMap(ServerOptions.AUTH_USER_TOKENS));
        assert !this.tokens.containsKey(USER_ADMIN);
        this.tokens.put(USER_ADMIN, config.get(ServerOptions.AUTH_ADMIN_TOKEN));
    }

    /**
     * Verify if a user is legal
     * @param username  the username for authentication
     * @param password  the password for authentication
     * @return String No permission if return ROLE_NONE else return a role
     */
    @Override
    public UserWithRole authenticate(final String username,
                                     final String password,
                                     final String token) {
        E.checkArgumentNotNull(username,
                               "The username parameter can't be null");
        E.checkArgumentNotNull(password,
                               "The password parameter can't be null");
        E.checkArgument(token == null, "The token must be null");

        RolePermission role;
        if (password.equals(this.tokens.get(username))) {
            if (username.equals(USER_ADMIN)) {
                role = ROLE_ADMIN;
            } else {
                // Return role with all permission, set user name as owner graph
                role = RolePermission.all(username);
            }
        } else {
            role = ROLE_NONE;
        }

        return new UserWithRole(IdGenerator.of(username), username, role);
    }

    @Override
    public AuthManager authManager() {
        throw new NotImplementedException(
                  "AuthManager is unsupported by ConfigAuthenticator");
    }

    @Override
    public void initAdminUser(String password) throws Exception {
        String adminToken = this.tokens.get(USER_ADMIN);
        E.checkArgument(Objects.equals(adminToken, password),
                        "The password can't be changed for " +
                        "ConfigAuthenticator");
    }

    @Override
    public SaslNegotiator newSaslNegotiator(InetAddress remoteAddress) {
        throw new NotImplementedException(
                  "SaslNegotiator is unsupported by ConfigAuthenticator");
    }
}
