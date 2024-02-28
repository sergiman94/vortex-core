package com.vortex.client.driver;

import com.vortex.client.client.RestClient;
import com.vortex.common.rest.ClientException;
import com.vortex.common.util.VersionUtil;
import com.vortex.client.version.ClientVersion;
import javax.ws.rs.ProcessingException;

import java.io.Closeable;

public class VortexClient implements Closeable {

    static {
        ClientVersion.check();
    }
    private final RestClient client;
    private final boolean borrowedClient;

    private VersionManager version;
    private GraphsManager graphs;
    private SchemaManager schema;
    private GraphManager graph;
    private GremlinManager gremlin;
    private TraverserManager traverser;
    private VariablesManager variable;
    private JobManager job;
    private TaskManager task;
    private AuthManager auth;
    private MetricsManager metrics;

    public VortexClient(VortexClientBuilder builder) {
        this.borrowedClient = false;
        try {
            this.client = new RestClient(builder.url(),
                                         builder.username(),
                                         builder.password(),
                                         builder.timeout(),
                                         builder.maxConns(),
                                         builder.maxConnsPerRoute(),
                                         builder.trustStoreFile(),
                                         builder.trustStorePassword());
        } catch (ProcessingException e) {
            throw new ClientException("Failed to connect url '%s'",
                                      builder.url());
        }
        try {
            this.initManagers(this.client, builder.graph());
        } catch (Throwable e) {
            this.client.close();
            throw e;
        }
    }

    public VortexClient(VortexClient client, String graph) {
        this.borrowedClient = true;
        this.client = client.client;
        this.initManagers(this.client, graph);
    }

    public static VortexClientBuilder builder(String url, String graph) {
        return new VortexClientBuilder(url, graph);
    }

    @Override
    public void close() {
        if (!this.borrowedClient) {
            this.client.close();
        }
    }

    private void initManagers(RestClient client, String graph) {
        assert client != null;
        // Check hugegraph-server api version
        this.version = new VersionManager(client);
        this.checkServerApiVersion();

        this.graphs = new GraphsManager(client);
        this.schema = new SchemaManager(client, graph);
        this.graph = new GraphManager(client, graph);
        this.gremlin = new GremlinManager(client, graph, this.graph);
        this.traverser = new TraverserManager(client, this.graph);
        this.variable = new VariablesManager(client, graph);
        this.job = new JobManager(client, graph);
        this.task = new TaskManager(client, graph);
        this.auth = new AuthManager(client, graph);
        this.metrics = new MetricsManager(client);
    }

    private void checkServerApiVersion() {
        VersionUtil.Version apiVersion = VersionUtil.Version.of(
                                         this.version.getApiVersion());
        VersionUtil.check(apiVersion, "0.38", "0.68",
                          "hugegraph-api in server");
        this.client.apiVersion(apiVersion);
    }

    public GraphsManager graphs() {
        return this.graphs;
    }

    public SchemaManager schema() {
        return this.schema;
    }

    public GraphManager graph() {
        return this.graph;
    }

    public GremlinManager gremlin() {
        return this.gremlin;
    }

    public TraverserManager traverser() {
        return this.traverser;
    }

    public VariablesManager variables() {
        return this.variable;
    }

    public JobManager job() {
        return this.job;
    }

    public TaskManager task() {
        return this.task;
    }

    public AuthManager auth() {
        return this.auth;
    }

    public MetricsManager metrics() {
        return this.metrics;
    }

    public void setAuthContext(String auth) {
        this.client.setAuthContext(auth);
    }

    public String getAuthContext() {
        return this.client.getAuthContext();
    }

    public void resetAuthContext() {
        this.client.resetAuthContext();
    }
}
