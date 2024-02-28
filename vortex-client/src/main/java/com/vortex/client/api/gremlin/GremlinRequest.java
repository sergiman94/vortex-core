package com.vortex.client.api.gremlin;

import com.vortex.client.driver.GremlinManager;
import com.vortex.client.structure.gremlin.ResultSet;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class GremlinRequest {

    // See org.apache.tinkerpop.gremlin.server.channel.HttpChannelizer
    public String gremlin;
    public Map<String, Object> bindings;
    public String language;
    public Map<String, String> aliases;

    public GremlinRequest(String gremlin) {
        this.gremlin = gremlin;
        this.bindings = new ConcurrentHashMap<>();
        this.language = "gremlin-groovy";
        this.aliases = new ConcurrentHashMap<>();
    }

    public static class Builder {
        private GremlinRequest request;
        private GremlinManager manager;

        public Builder(String gremlin, GremlinManager executor) {
            this.request = new GremlinRequest(gremlin);
            this.manager = executor;
        }

        public ResultSet execute() {
            return this.manager.execute(this.request);
        }

        public long executeAsTask() {
            return this.manager.executeAsTask(this.request);
        }

        public Builder binding(String key, Object value) {
            this.request.bindings.put(key, value);
            return this;
        }

        public Builder language(String language) {
            this.request.language = language;
            return this;
        }

        public Builder alias(String key, String value) {
            this.request.aliases.put(key, value);
            return this;
        }
    }
}
