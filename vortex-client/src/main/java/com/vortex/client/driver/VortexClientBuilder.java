package com.vortex.client.driver;

import com.vortex.common.util.E;

public class VortexClientBuilder {

    private static final int CPUS = Runtime.getRuntime().availableProcessors();
    private static final int DEFAULT_TIMEOUT = 20;
    private static final int DEFAULT_MAX_CONNS = 4 * CPUS;
    private static final int DEFAULT_MAX_CONNS_PER_ROUTE = 2 * CPUS;
    private static final int DEFAULT_IDLE_TIME = 30;

    private String url;
    private String graph;
    private String username;
    private String password;
    private int timeout;
    private int maxConns;
    private int maxConnsPerRoute;
    private int idleTime;
    private String trustStoreFile;
    private String trustStorePassword;

    public VortexClientBuilder(String url, String graph) {
        E.checkArgument(url != null && !url.isEmpty(),
                        "Expect a string value as the url " +
                        "parameter argument, but got: %s", url);
        E.checkArgument(graph != null && !graph.isEmpty(),
                        "Expect a string value as the graph name " +
                        "parameter argument, but got: %s", graph);
        this.url = url;
        this.graph = graph;
        this.username = "";
        this.password = "";
        this.timeout = DEFAULT_TIMEOUT;
        this.maxConns = DEFAULT_MAX_CONNS;
        this.maxConnsPerRoute = DEFAULT_MAX_CONNS_PER_ROUTE;
        this.trustStoreFile = "";
        this.trustStorePassword = "";
        this.idleTime = DEFAULT_IDLE_TIME;
    }

    public VortexClient build() {
        E.checkArgument(this.url != null,
                        "The url parameter can't be null");
        E.checkArgument(this.graph != null,
                        "The graph parameter can't be null");
        return new VortexClient(this);
    }

    public VortexClientBuilder configGraph(String graph) {
        this.graph = graph;
        return this;
    }

    public VortexClientBuilder configIdleTime(int idleTime) {
        E.checkArgument(idleTime > 0,
                        "The idleTime parameter must be > 0, " +
                        "but got %s", idleTime);
        this.idleTime = idleTime;
        return this;
    }

    public VortexClientBuilder configPool(int maxConns, int maxConnsPerRoute) {
        if (maxConns == 0) {
            maxConns = DEFAULT_MAX_CONNS;
        }
        if (maxConnsPerRoute == 0) {
            maxConnsPerRoute = DEFAULT_MAX_CONNS_PER_ROUTE;
        }
        this.maxConns = maxConns;
        this.maxConnsPerRoute = maxConnsPerRoute;
        return this;
    }

    public VortexClientBuilder configSSL(String trustStoreFile,
                                         String trustStorePassword) {
        this.trustStoreFile = trustStoreFile;
        this.trustStorePassword = trustStorePassword;
        return this;
    }

    public VortexClientBuilder configTimeout(int timeout) {
        if (timeout == 0) {
            timeout = DEFAULT_TIMEOUT;
        }
        this.timeout = timeout;
        return this;
    }

    public VortexClientBuilder configUrl(String url) {
        this.url = url;
        return this;
    }

    public VortexClientBuilder configUser(String username, String password) {
        if (username == null) {
            username = "";
        }
        if (password == null) {
            password = "";
        }
        this.username = username;
        this.password = password;

        return this;
    }

    public String url() {
        return this.url;
    }

    public String graph() {
        return this.graph;
    }

    public String username() {
        return this.username;
    }

    public String password() {
        return this.password;
    }

    public int timeout() {
        return this.timeout;
    }

    public int maxConns() {
        return maxConns;
    }

    public int maxConnsPerRoute() {
        return this.maxConnsPerRoute;
    }

    public int idleTime() {
        return this.idleTime;
    }

    public String trustStoreFile() {
        return this.trustStoreFile;
    }

    public String trustStorePassword() {
        return this.trustStorePassword;
    }
}
