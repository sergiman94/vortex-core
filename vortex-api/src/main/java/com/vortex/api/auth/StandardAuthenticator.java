
package com.vortex.api.auth;

import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.auth.*;
import com.vortex.vortexdb.config.CoreOptions;
import com.vortex.common.config.VortexConfig;
import com.vortex.api.config.ServerOptions;
import com.vortex.api.rpc.RpcClientProviderWithAuth;
import com.vortex.vortexdb.util.ConfigUtil;
import com.vortex.common.util.E;
import com.vortex.vortexdb.util.StringEncoding;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;

import java.io.Console;
import java.net.InetAddress;
import java.util.Map;
import java.util.Scanner;

public class StandardAuthenticator implements VortexAuthenticator {

    private static final String INITING_STORE = "initing_store";

    private Vortex graph = null;

    private Vortex graph() {
        E.checkState(this.graph != null, "Must setup Authenticator first");
        return this.graph;
    }

    private void initAdminUser() throws Exception {
        if (this.requireInitAdminUser()) {
            this.initAdminUser(this.inputPassword());
        }
        this.graph.close();
    }

    @Override
    public void initAdminUser(String password) {
        // Not allowed to call by non main thread
        String caller = Thread.currentThread().getName();
        E.checkState("main".equals(caller), "Invalid caller '%s'", caller);

        AuthManager authManager = this.graph().graph().authManager();
        // Only init user when local mode and user has not been initialized
        if (this.requireInitAdminUser()) {
            VortexUser admin = new VortexUser(VortexAuthenticator.USER_ADMIN);
            admin.password(StringEncoding.hashPassword(password));
            admin.creator(VortexAuthenticator.USER_SYSTEM);
            authManager.createUser(admin);
        }
    }

    private boolean requireInitAdminUser() {
        AuthManager authManager = this.graph().graph().authManager();
        return StandardAuthManager.isLocal(authManager) &&
               authManager.findUser(VortexAuthenticator.USER_ADMIN) == null;
    }

    private String inputPassword() {
        String inputPrompt = "Please input the admin password:";
        String notEmptyPrompt = "The admin password can't be empty";
        Console console = System.console();
        while (true) {
            String password = "";
            if (console != null) {
                char[] chars = console.readPassword(inputPrompt);
                password = new String(chars);
            } else {
                System.out.print(inputPrompt);
                @SuppressWarnings("resource") // just wrapper of System.in
                Scanner scanner = new Scanner(System.in);
                password = scanner.nextLine();
            }
            if (!password.isEmpty()) {
                return password;
            }
            System.out.println(notEmptyPrompt);
        }
    }

    @Override
    public void setup(VortexConfig config) {
        String graphName = config.get(ServerOptions.AUTH_GRAPH_STORE);
        Map<String, String> graphConfs = ConfigUtil.scanGraphsDir(
                                         config.get(ServerOptions.GRAPHS));
        String graphPath = graphConfs.get(graphName);
        E.checkArgument(graphPath != null,
                        "Can't find graph name '%s' in config '%s' at " +
                        "'rest-server.properties' to store auth information, " +
                        "please ensure the value of '%s' matches it correctly",
                        graphName, ServerOptions.GRAPHS,
                        ServerOptions.AUTH_GRAPH_STORE.name());

        VortexConfig graphConfig = new VortexConfig(graphPath);
        if (config.getProperty(INITING_STORE) != null &&
            config.getBoolean(INITING_STORE)) {
            // Forced set RAFT_MODE to false when initializing backend
            graphConfig.setProperty(CoreOptions.RAFT_MODE.name(), "false");
        }
        this.graph = (Vortex) GraphFactory.open(graphConfig);

        String remoteUrl = config.get(ServerOptions.AUTH_REMOTE_URL);
        if (StringUtils.isNotEmpty(remoteUrl)) {
            RpcClientProviderWithAuth clientProvider =
                                      new RpcClientProviderWithAuth(config);
            this.graph.switchAuthManager(clientProvider.authManager());
        }
    }

    /**
     * Verify if a user is legal
     * @param username the username for authentication
     * @param password the password for authentication
     * @param token the token for authentication
     * @return String No permission if return ROLE_NONE else return a role
     */
    @Override
    public UserWithRole authenticate(String username, String password,
                                     String token) {
        UserWithRole userWithRole;
        if (StringUtils.isNotEmpty(token)) {
            userWithRole = this.authManager().validateUser(token);
        } else {
            E.checkArgumentNotNull(username,
                                   "The username parameter can't be null");
            E.checkArgumentNotNull(password,
                                   "The password parameter can't be null");
            userWithRole = this.authManager().validateUser(username, password);
        }

        RolePermission role = userWithRole.role();
        if (role == null) {
            role = ROLE_NONE;
        } else if (USER_ADMIN.equals(userWithRole.username())) {
            role = ROLE_ADMIN;
        } else {
            return userWithRole;
        }

        return new UserWithRole(userWithRole.userId(),
                                userWithRole.username(), role);
    }

    @Override
    public AuthManager authManager() {
        return this.graph().authManager();
    }

    @Override
    public SaslNegotiator newSaslNegotiator(InetAddress remoteAddress) {
        throw new NotImplementedException("SaslNegotiator is unsupported");
    }

    public static void initAdminUserIfNeeded(String confFile) throws Exception {
        StandardAuthenticator auth = new StandardAuthenticator();
        VortexConfig config = new VortexConfig(confFile);
        String authClass = config.get(ServerOptions.AUTHENTICATOR);
        if (authClass.isEmpty()) {
            return;
        }
        config.addProperty(INITING_STORE, true);
        auth.setup(config);
        if (auth.graph().backendStoreFeatures().supportsPersistence()) {
            auth.initAdminUser();
        }
    }
}
