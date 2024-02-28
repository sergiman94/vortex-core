package com.vortex.client.driver;

import com.vortex.client.api.version.VersionAPI;
import com.vortex.client.client.RestClient;
import com.vortex.client.structure.version.Versions;

public class VersionManager {

    private VersionAPI versionAPI;

    public VersionManager(RestClient client) {
        this.versionAPI = new VersionAPI(client);
    }

    public String getCoreVersion() {
        Versions versions = this.versionAPI.get();
        return versions.get("core");
    }

    public String getGremlinVersion() {
        Versions versions = this.versionAPI.get();
        return versions.get("gremlin");
    }

    public String getApiVersion() {
        Versions versions = this.versionAPI.get();
        return versions.get("api");
    }
}
