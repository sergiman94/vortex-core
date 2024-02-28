
package com.vortex.api.api.traversers;

import com.vortex.vortexdb.Vortex;
import com.vortex.api.api.API;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.api.core.GraphManager;
import com.vortex.api.server.RestServer;
import com.vortex.vortexdb.structure.VortexVertex;
import com.vortex.vortexdb.traversal.algorithm.CountTraverser;
import com.vortex.vortexdb.traversal.algorithm.steps.EdgeStep;
import com.vortex.vortexdb.type.define.Directions;
import com.vortex.common.util.E;
import com.vortex.common.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;

import javax.inject.Singleton;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.vortex.vortexdb.traversal.algorithm.VortexTraverser.*;

@Path("graphs/{graph}/traversers/count")
@Singleton
public class CountAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String post(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       CountRequest request) {
        LOG.debug("Graph [{}] get count from '{}' with request {}",
                  graph, request);

        E.checkArgumentNotNull(request.source,
                               "The source of request can't be null");
        Id sourceId = VortexVertex.getIdValue(request.source);
        E.checkArgumentNotNull(request.steps != null &&
                               !request.steps.isEmpty(),
                               "The steps of request can't be null or empty");
        E.checkArgumentNotNull(request.dedupSize == NO_LIMIT ||
                               request.dedupSize >= 0L,
                               "The dedup size of request " +
                               "must >= 0 or == -1, but got: '%s'",
                               request.dedupSize);

        Vortex g = graph(manager, graph);
        List<EdgeStep> steps = steps(g, request);
        CountTraverser traverser = new CountTraverser(g);
        long count = traverser.count(sourceId, steps, request.containsTraversed,
                                     request.dedupSize);

        return manager.serializer(g).writeMap(ImmutableMap.of("count", count));
    }

    private static List<EdgeStep> steps(Vortex graph, CountRequest request) {
        int stepSize = request.steps.size();
        List<EdgeStep> steps = new ArrayList<>(stepSize);
        for (Step step : request.steps) {
            steps.add(step.jsonToStep(graph));
        }
        return steps;
    }

    private static class CountRequest {

        @JsonProperty("source")
        public Object source;
        @JsonProperty("steps")
        public List<Step> steps;
        @JsonProperty("contains_traversed")
        public boolean containsTraversed = false;
        @JsonProperty("dedup_size")
        public long dedupSize = 1000000L;

        @Override
        public String toString() {
            return String.format("CountRequest{source=%s,steps=%s," +
                                 "containsTraversed=%s,dedupSize=%s}",
                                 this.source, this.steps,
                                 this.containsTraversed, this.dedupSize);
        }
    }

    private static class Step {

        @JsonProperty("direction")
        public Directions direction = Directions.BOTH;
        @JsonProperty("labels")
        public List<String> labels;
        @JsonProperty("properties")
        public Map<String, Object> properties;
        @JsonAlias("degree")
        @JsonProperty("max_degree")
        public long maxDegree = Long.parseLong(DEFAULT_MAX_DEGREE);
        @JsonProperty("skip_degree")
        public long skipDegree = Long.parseLong(DEFAULT_SKIP_DEGREE);

        @Override
        public String toString() {
            return String.format("Step{direction=%s,labels=%s,properties=%s" +
                                 "maxDegree=%s,skipDegree=%s}",
                                 this.direction, this.labels, this.properties,
                                 this.maxDegree, this.skipDegree);
        }

        private EdgeStep jsonToStep(Vortex graph) {
            return new EdgeStep(graph, this.direction, this.labels,
                                this.properties, this.maxDegree,
                                this.skipDegree);
        }
    }
}
