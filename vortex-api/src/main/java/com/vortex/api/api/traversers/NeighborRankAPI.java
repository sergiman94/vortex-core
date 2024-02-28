
package com.vortex.api.api.traversers;

import com.vortex.vortexdb.Vortex;
import com.vortex.api.api.API;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.api.core.GraphManager;
import com.vortex.api.server.RestServer;
import com.vortex.vortexdb.structure.VortexVertex;
import com.vortex.vortexdb.traversal.algorithm.NeighborRankTraverser;
import com.vortex.vortexdb.type.define.Directions;
import com.vortex.common.util.E;
import com.vortex.common.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
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

@Path("graphs/{graph}/traversers/neighborrank")
@Singleton
public class NeighborRankAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String neighborRank(@Context GraphManager manager,
                               @PathParam("graph") String graph,
                               RankRequest request) {
        E.checkArgumentNotNull(request, "The rank request body can't be null");
        E.checkArgumentNotNull(request.source,
                               "The source of rank request can't be null");
        E.checkArgument(request.steps != null && !request.steps.isEmpty(),
                        "The steps of rank request can't be empty");
        E.checkArgument(request.steps.size() <= DEFAULT_MAX_DEPTH,
                        "The steps length of rank request can't exceed %s",
                        DEFAULT_MAX_DEPTH);
        E.checkArgument(request.alpha > 0 && request.alpha <= 1.0,
                        "The alpha of rank request must be in range (0, 1], " +
                        "but got '%s'", request.alpha);

        LOG.debug("Graph [{}] get neighbor rank from '{}' with steps '{}', " +
                  "alpha '{}' and capacity '{}'", graph, request.source,
                  request.steps, request.alpha, request.capacity);

        Id sourceId = VortexVertex.getIdValue(request.source);
        Vortex g = graph(manager, graph);

        List<NeighborRankTraverser.Step> steps = steps(g, request);
        NeighborRankTraverser traverser;
        traverser = new NeighborRankTraverser(g, request.alpha,
                                              request.capacity);
        List<Map<Id, Double>> ranks = traverser.neighborRank(sourceId, steps);
        return manager.serializer(g).writeList("ranks", ranks);
    }

    private static List<NeighborRankTraverser.Step> steps(Vortex graph,
                                                          RankRequest req) {
        List<NeighborRankTraverser.Step> steps = new ArrayList<>();
        for (Step step : req.steps) {
            steps.add(step.jsonToStep(graph));
        }
        return steps;
    }

    private static class RankRequest {

        @JsonProperty("source")
        private Object source;
        @JsonProperty("steps")
        private List<Step> steps;
        @JsonProperty("alpha")
        private double alpha;
        @JsonProperty("capacity")
        public long capacity = Long.parseLong(DEFAULT_CAPACITY);

        @Override
        public String toString() {
            return String.format("RankRequest{source=%s,steps=%s,alpha=%s," +
                                 "capacity=%s}", this.source, this.steps,
                                 this.alpha, this.capacity);
        }
    }

    private static class Step {

        @JsonProperty("direction")
        public Directions direction;
        @JsonProperty("labels")
        public List<String> labels;
        @JsonAlias("degree")
        @JsonProperty("max_degree")
        public long maxDegree = Long.parseLong(DEFAULT_MAX_DEGREE);
        @JsonProperty("skip_degree")
        public long skipDegree = 0L;
        @JsonProperty("top")
        public int top = Integer.parseInt(DEFAULT_PATHS_LIMIT);

        public static final int DEFAULT_CAPACITY_PER_LAYER = 100000;

        @Override
        public String toString() {
            return String.format("Step{direction=%s,labels=%s,maxDegree=%s," +
                                 "top=%s}", this.direction, this.labels,
                                 this.maxDegree, this.top);
        }

        private NeighborRankTraverser.Step jsonToStep(Vortex g) {
            return new NeighborRankTraverser.Step(g, this.direction,
                                                  this.labels,
                                                  this.maxDegree,
                                                  this.skipDegree,
                                                  this.top,
                                                  DEFAULT_CAPACITY_PER_LAYER);
        }
    }
}
