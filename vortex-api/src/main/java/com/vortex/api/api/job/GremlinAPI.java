
package com.vortex.api.api.job;

import com.vortex.vortexdb.VortexException;
import com.vortex.vortexdb.Vortex;
import com.vortex.api.api.API;
import com.vortex.api.api.filter.StatusFilter.Status;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.api.core.GraphManager;
import com.vortex.api.define.Checkable;
import com.vortex.vortexdb.job.GremlinJob;
import com.vortex.vortexdb.job.JobBuilder;
import com.vortex.api.metrics.MetricsUtil;
import com.vortex.api.server.RestServer;
import com.vortex.common.util.E;
import com.vortex.vortexdb.util.JsonUtil;
import com.vortex.common.util.Log;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;

import javax.annotation.security.RolesAllowed;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.HashMap;
import java.util.Map;

@Path("graphs/{graph}/jobs/gremlin")
@Singleton
public class GremlinAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    private static final int MAX_NAME_LENGTH = 256;

    private static final Histogram gremlinJobInputHistogram =
            MetricsUtil.registerHistogram(GremlinAPI.class, "gremlin-input");

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$graph $action=gremlin_execute"})
    public Map<String, Id> post(@Context GraphManager manager,
                                @PathParam("graph") String graph,
                                GremlinRequest request) {
        LOG.debug("Graph [{}] schedule gremlin job: {}", graph, request);
        checkCreatingBody(request);
        gremlinJobInputHistogram.update(request.gremlin.length());

        Vortex g = graph(manager, graph);
        request.aliase(graph, "graph");
        JobBuilder<Object> builder = JobBuilder.of(g);
        builder.name(request.name())
               .input(request.toJson())
               .job(new GremlinJob());
        return ImmutableMap.of("task_id", builder.schedule().id());
    }

    public static class GremlinRequest implements Checkable {

        // See org.apache.tinkerpop.gremlin.server.channel.HttpChannelizer
        @JsonProperty
        private String gremlin;
        @JsonProperty
        private Map<String, Object> bindings = new HashMap<>();
        @JsonProperty
        private String language = "gremlin-groovy";
        @JsonProperty
        private Map<String, String> aliases = new HashMap<>();

        public String gremlin() {
            return this.gremlin;
        }

        public void gremlin(String gremlin) {
            this.gremlin = gremlin;
        }

        public Map<String, Object> bindings() {
            return this.bindings;
        }

        public void bindings(Map<String, Object> bindings) {
            this.bindings = bindings;
        }

        public void binding(String name, Object value) {
            this.bindings.put(name, value);
        }

        public String language() {
            return this.language;
        }

        public void language(String language) {
            this.language = language;
        }

        public Map<String, String> aliases() {
            return this.aliases;
        }

        public void aliases(Map<String, String> aliases) {
            this.aliases = aliases;
        }

        public void aliase(String key, String value) {
            this.aliases.put(key, value);
        }

        public String name() {
            // Get the first line of script as the name
            String firstLine = this.gremlin.split("\r\n|\r|\n", 2)[0];
            final Charset charset = Charset.forName(CHARSET);
            final byte[] bytes = firstLine.getBytes(charset);
            if (bytes.length <= MAX_NAME_LENGTH) {
                return firstLine;
            }

            /*
             * Reference https://stackoverflow.com/questions/3576754/truncating-strings-by-bytes
             */
            CharsetDecoder decoder = charset.newDecoder();
            decoder.onMalformedInput(CodingErrorAction.IGNORE);
            decoder.reset();

            ByteBuffer buffer = ByteBuffer.wrap(bytes, 0, MAX_NAME_LENGTH);
            try {
                return decoder.decode(buffer).toString();
            } catch (CharacterCodingException e) {
                throw new VortexException("Failed to decode truncated bytes of " +
                                        "gremlin first line", e);
            }
        }

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgumentNotNull(this.gremlin,
                                   "The gremlin parameter can't be null");
            E.checkArgumentNotNull(this.language,
                                   "The language parameter can't be null");
            E.checkArgument(this.aliases == null || this.aliases.isEmpty(),
                            "There is no need to pass gremlin aliases");
        }

        public String toJson() {
            Map<String, Object> map = new HashMap<>();
            map.put("gremlin", this.gremlin);
            map.put("bindings", this.bindings);
            map.put("language", this.language);
            map.put("aliases", this.aliases);
            return JsonUtil.toJson(map);
        }

        public static GremlinRequest fromJson(String json) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = JsonUtil.fromJson(json, Map.class);
            String gremlin = (String) map.get("gremlin");
            @SuppressWarnings("unchecked")
            Map<String, Object> bindings = (Map<String, Object>)
                                           map.get("bindings");
            String language = (String) map.get("language");
            @SuppressWarnings("unchecked")
            Map<String, String> aliases = (Map<String, String>)
                                          map.get("aliases");

            GremlinRequest request = new GremlinRequest();
            request.gremlin(gremlin);
            request.bindings(bindings);
            request.language(language);
            request.aliases(aliases);
            return request;
        }
    }
}
