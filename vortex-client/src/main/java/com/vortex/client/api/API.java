package com.vortex.client.api;

import com.vortex.client.client.RestClient;
import com.vortex.common.util.E;

public abstract class API {

    public static final String CHARSET = "UTF-8";
    public static final String BATCH_ENCODING = "gzip";
    public static final long NO_LIMIT = -1L;
    public static final String PATH_SPLITOR = "/";

    protected final RestClient client;

    private String path;

    public API(RestClient client) {
        E.checkNotNull(client, "client");
        this.client = client;
        this.path = null;
    }

    public String path() {
        E.checkState(this.path != null, "Path can't be null");
        return this.path;
    }

    protected void path(String path) {
        this.path = path;
    }

    protected void path(String pathTemplate, Object... args) {
        this.path = String.format(pathTemplate, args);
    }

    protected abstract String type();

    protected static void checkOffset(long value) {
        E.checkArgument(value >= 0, "Offset must be >= 0, but got: %s", value);
    }

    protected static void checkLimit(long value, String name) {
        E.checkArgument(value > 0 || value == NO_LIMIT,
                        "%s must be > 0 or == %s, but got: %s",
                        name, NO_LIMIT, value);
    }
}
