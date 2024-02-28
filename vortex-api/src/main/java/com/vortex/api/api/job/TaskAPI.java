
package com.vortex.api.api.job;

import com.vortex.api.api.API;
import com.vortex.api.api.filter.StatusFilter.Status;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.id.IdGenerator;
import com.vortex.vortexdb.backend.page.PageInfo;
import com.vortex.api.core.GraphManager;
import com.vortex.api.server.RestServer;
import com.vortex.vortexdb.task.VortexTask;
import com.vortex.vortexdb.task.TaskScheduler;
import com.vortex.vortexdb.task.TaskStatus;
import com.vortex.common.util.E;
import com.vortex.common.util.Log;
import com.codahale.metrics.annotation.Timed;
import org.apache.groovy.util.Maps;
import org.slf4j.Logger;

import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import java.util.*;
import java.util.stream.Collectors;

@Path("graphs/{graph}/tasks")
@Singleton
public class TaskAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);
    private static final long NO_LIMIT = -1L;

    public static final String ACTION_CANCEL = "cancel";

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, Object> list(@Context GraphManager manager,
                                    @PathParam("graph") String graph,
                                    @QueryParam("status") String status,
                                    @QueryParam("ids") List<Long> ids,
                                    @QueryParam("limit")
                                    @DefaultValue("100") long limit,
                                    @QueryParam("page") String page) {
        LOG.debug("Graph [{}] list tasks with status {}, ids {}, " +
                  "limit {}, page {}", graph, status, ids, limit, page);

        TaskScheduler scheduler = graph(manager, graph).taskScheduler();

        Iterator<VortexTask<Object>> iter;

        if (!ids.isEmpty()) {
            E.checkArgument(status == null,
                            "Not support status when query task by ids, " +
                            "but got status='%s'", status);
            E.checkArgument(page == null,
                            "Not support page when query task by ids, " +
                            "but got page='%s'", page);
            // Set limit to NO_LIMIT to ignore limit when query task by ids
            limit = NO_LIMIT;
            List<Id> idList = ids.stream().map(IdGenerator::of)
                                          .collect(Collectors.toList());
            iter = scheduler.tasks(idList);
        } else {
            if (status == null) {
                iter = scheduler.tasks(null, limit, page);
            } else {
                iter = scheduler.tasks(parseStatus(status), limit, page);
            }
        }

        List<Object> tasks = new ArrayList<>();
        while (iter.hasNext()) {
            tasks.add(iter.next().asMap(false));
        }
        if (limit != NO_LIMIT && tasks.size() > limit) {
            tasks = tasks.subList(0, (int) limit);
        }

        if (page == null) {
            return Maps.of("tasks", tasks);
        } else {
            return Maps.of("tasks", tasks, "page", PageInfo.pageInfo(iter));
        }
    }

    @GET
    @Timed
    @Path("{id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, Object> get(@Context GraphManager manager,
                                   @PathParam("graph") String graph,
                                   @PathParam("id") long id) {
        LOG.debug("Graph [{}] get task: {}", graph, id);

        TaskScheduler scheduler = graph(manager, graph).taskScheduler();
        return scheduler.task(IdGenerator.of(id)).asMap();
    }

    @DELETE
    @Timed
    @Path("{id}")
    public void delete(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @PathParam("id") long id) {
        LOG.debug("Graph [{}] delete task: {}", graph, id);

        TaskScheduler scheduler = graph(manager, graph).taskScheduler();
        VortexTask<?> task = scheduler.delete(IdGenerator.of(id));
        E.checkArgument(task != null, "There is no task with id '%s'", id);
    }

    @PUT
    @Timed
    @Path("{id}")
    @Status(Status.ACCEPTED)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, Object> update(@Context GraphManager manager,
                                      @PathParam("graph") String graph,
                                      @PathParam("id") long id,
                                      @QueryParam("action") String action) {
        LOG.debug("Graph [{}] cancel task: {}", graph, id);

        if (!ACTION_CANCEL.equals(action)) {
            throw new NotSupportedException(String.format(
                      "Not support action '%s'", action));
        }

        TaskScheduler scheduler = graph(manager, graph).taskScheduler();
        VortexTask<?> task = scheduler.task(IdGenerator.of(id));
        if (!task.completed() && !task.cancelling()) {
            scheduler.cancel(task);
            if (task.cancelling() || task.cancelled()) {
                return task.asMap();
            }
        }

        assert task.completed() || task.cancelling();
        throw new BadRequestException(String.format(
                  "Can't cancel task '%s' which is completed or cancelling",
                  id));
    }

    private static TaskStatus parseStatus(String status) {
        try {
            return TaskStatus.valueOf(status);
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format(
                      "Status value must be in %s, but got '%s'",
                      Arrays.asList(TaskStatus.values()), status));
        }
    }
}