
package com.vortex.api.api.traversers;

import com.vortex.vortexdb.Vortex;
import com.vortex.api.api.API;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.api.core.GraphManager;
import com.vortex.api.server.RestServer;
import com.vortex.vortexdb.structure.VortexVertex;
import com.vortex.vortexdb.traversal.algorithm.VortexTraverser;
import com.vortex.vortexdb.traversal.algorithm.PersonalRankTraverser;
import com.vortex.common.util.E;
import com.vortex.common.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;

import javax.inject.Singleton;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import java.util.Map;

import static com.vortex.vortexdb.traversal.algorithm.VortexTraverser.*;

@Path("graphs/{graph}/traversers/personalrank")
@Singleton
public class PersonalRankAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    private static final double DEFAULT_DIFF = 0.0001;
    private static final double DEFAULT_ALPHA = 0.85;
    private static final int DEFAULT_DEPTH = 5;

    @POST
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String personalRank(@Context GraphManager manager,
                               @PathParam("graph") String graph,
                               RankRequest request) {
        E.checkArgumentNotNull(request, "The rank request body can't be null");
        E.checkArgument(request.source != null,
                        "The source vertex id of rank request can't be null");
        E.checkArgument(request.label != null,
                        "The edge label of rank request can't be null");
        E.checkArgument(request.alpha > 0 && request.alpha <= 1.0,
                        "The alpha of rank request must be in range (0, 1], " +
                        "but got '%s'", request.alpha);
        E.checkArgument(request.maxDiff > 0 && request.maxDiff <= 1.0,
                        "The max diff of rank request must be in range " +
                        "(0, 1], but got '%s'", request.maxDiff);
        E.checkArgument(request.maxDegree > 0L || request.maxDegree == NO_LIMIT,
                        "The max degree of rank request must be > 0 " +
                        "or == -1, but got: %s", request.maxDegree);
        E.checkArgument(request.limit > 0L || request.limit == NO_LIMIT,
                        "The limit of rank request must be > 0 or == -1, " +
                        "but got: %s", request.limit);
        E.checkArgument(request.maxDepth > 1L &&
                        request.maxDepth <= DEFAULT_MAX_DEPTH,
                        "The max depth of rank request must be " +
                        "in range (1, %s], but got '%s'",
                        DEFAULT_MAX_DEPTH, request.maxDepth);

        LOG.debug("Graph [{}] get personal rank from '{}' with " +
                  "edge label '{}', alpha '{}', maxDegree '{}', " +
                  "max depth '{}' and sorted '{}'",
                  graph, request.source, request.label, request.alpha,
                  request.maxDegree, request.maxDepth, request.sorted);

        Id sourceId = VortexVertex.getIdValue(request.source);
        Vortex g = graph(manager, graph);

        PersonalRankTraverser traverser;
        traverser = new PersonalRankTraverser(g, request.alpha, request.maxDegree,
                                              request.maxDepth);
        Map<Id, Double> ranks = traverser.personalRank(sourceId, request.label,
                                                       request.withLabel);
        ranks = VortexTraverser.topN(ranks, request.sorted, request.limit);
        return manager.serializer(g).writeMap(ranks);
    }

    private static class RankRequest {

        @JsonProperty("source")
        private Object source;
        @JsonProperty("label")
        private String label;
        @JsonProperty("alpha")
        private double alpha = DEFAULT_ALPHA;
        // TODO: used for future enhancement
        @JsonProperty("max_diff")
        private double maxDiff = DEFAULT_DIFF;
        @JsonProperty("max_degree")
        private long maxDegree = Long.parseLong(DEFAULT_MAX_DEGREE);
        @JsonProperty("limit")
        private long limit = Long.parseLong(DEFAULT_LIMIT);
        @JsonProperty("max_depth")
        private int maxDepth = DEFAULT_DEPTH;
        @JsonProperty("with_label")
        private PersonalRankTraverser.WithLabel withLabel =
                PersonalRankTraverser.WithLabel.BOTH_LABEL;
        @JsonProperty("sorted")
        private boolean sorted = true;

        @Override
        public String toString() {
            return String.format("RankRequest{source=%s,label=%s,alpha=%s," +
                                 "maxDiff=%s,maxDegree=%s,limit=%s," +
                                 "maxDepth=%s,withLabel=%s,sorted=%s}",
                                 this.source, this.label, this.alpha,
                                 this.maxDiff, this.maxDegree, this.limit,
                                 this.maxDepth, this.withLabel, this.sorted);
        }
    }
}
