
package com.vortex.api.api.job;

import com.vortex.vortexdb.Vortex;
import com.vortex.api.api.API;
import com.vortex.api.api.filter.StatusFilter.Status;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.api.core.GraphManager;
import com.vortex.vortexdb.job.ComputerJob;
import com.vortex.vortexdb.job.JobBuilder;
import com.vortex.api.server.RestServer;
import com.vortex.vortexdb.task.VortexTask;
import com.vortex.common.util.E;
import com.vortex.vortexdb.util.JsonUtil;
import com.vortex.common.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;

import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import java.util.Map;

@Path("graphs/{graph}/jobs/computer")
@Singleton
public class ComputerAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Path("/{name}")
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, Id> post(@Context GraphManager manager,
                                @PathParam("graph") String graph,
                                @PathParam("name") String computer,
                                Map<String, Object> parameters) {
        LOG.debug("Graph [{}] schedule computer job: {}", graph, parameters);
        E.checkArgument(computer != null && !computer.isEmpty(),
                        "The computer name can't be empty");
        if (parameters == null) {
            parameters = ImmutableMap.of();
        }
        if (!ComputerJob.check(computer, parameters)) {
            throw new NotFoundException("Not found computer: " + computer);
        }

        Vortex g = graph(manager, graph);
        Map<String, Object> input = ImmutableMap.of("computer", computer,
                                                    "parameters", parameters);
        JobBuilder<Object> builder = JobBuilder.of(g);
        builder.name("computer:" + computer)
               .input(JsonUtil.toJson(input))
               .job(new ComputerJob());
        VortexTask<Object> task = builder.schedule();
        return ImmutableMap.of("task_id", task.id());
    }
}
