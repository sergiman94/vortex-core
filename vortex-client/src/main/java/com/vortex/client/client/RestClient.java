package com.vortex.client.client;

import com.vortex.client.exception.ServerException;
import com.vortex.common.rest.AbstractRestClient;
import com.vortex.common.rest.ClientException;
import com.vortex.common.rest.RestResult;
import com.vortex.client.serializer.PathDeserializer;
import com.vortex.client.structure.graph.Path;
import com.vortex.common.util.E;
import com.vortex.common.util.VersionUtil;
import com.vortex.common.util.VersionUtil.Version;
import com.fasterxml.jackson.databind.module.SimpleModule;
import javax.ws.rs.core.Response;

public class RestClient extends AbstractRestClient {

    private static final int SECOND = 1000;

    private Version apiVersion = null;

    static {
        SimpleModule module = new SimpleModule();
        module.addDeserializer(Path.class, new PathDeserializer());
        RestResult.registerModule(module);
    }

    public RestClient(String url, String username, String password,
                      int timeout) {
        super(url, username, password, timeout * SECOND);
    }

    public RestClient(String url, String username, String password, int timeout,
                      int maxConns, int maxConnsPerRoute,
                      String trustStoreFile, String trustStorePassword) {
        super(url, username, password, timeout * SECOND, maxConns,
              maxConnsPerRoute, trustStoreFile, trustStorePassword);
    }

    public void apiVersion(Version version) {
        E.checkNotNull(version, "api version");
        this.apiVersion = version;
    }

    public Version apiVersion() {
        return this.apiVersion;
    }

    public void checkApiVersion(String minVersion, String message) {
        if (this.apiVersionLt(minVersion)) {
            throw new ClientException(
                      "HugeGraphServer API version must be >= %s to support " +
                      "%s, but current HugeGraphServer API version is: %s",
                      minVersion, message, this.apiVersion.get());
        }
    }

    public boolean apiVersionLt(String minVersion) {
        String apiVersion = this.apiVersion == null ?
                            null : this.apiVersion.get();
        return apiVersion != null && !VersionUtil.gte(apiVersion, minVersion);
    }

    @Override
    protected void checkStatus(Response response, Response.Status... statuses) {
        boolean match = false;
        for (Response.Status status : statuses) {
            if (status.getStatusCode() == response.getStatus()) {
                match = true;
                break;
            }
        }
        if (!match) {
            throw ServerException.fromResponse(response);
        }
    }
}
