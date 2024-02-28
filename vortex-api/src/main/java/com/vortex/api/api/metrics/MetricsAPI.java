
package com.vortex.api.api.metrics;

import com.vortex.vortexdb.Vortex;
import com.vortex.api.api.API;
import com.vortex.vortexdb.backend.store.BackendMetrics;
import com.vortex.api.core.GraphManager;
import com.vortex.api.metrics.MetricsModule;
import com.vortex.api.metrics.ServerReporter;
import com.vortex.api.metrics.SystemMetrics;
import com.vortex.common.util.InsertionOrderUtil;
import com.vortex.vortexdb.util.JsonUtil;
import com.vortex.common.util.Log;
import com.codahale.metrics.Metric;
import com.codahale.metrics.annotation.Timed;
import org.slf4j.Logger;

import javax.annotation.security.RolesAllowed;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@Singleton
@Path("metrics")
public class MetricsAPI extends API {

    private static final Logger LOG = Log.logger(MetricsAPI.class);

    private SystemMetrics systemMetrics;

    static {
        JsonUtil.registerModule(new MetricsModule(SECONDS, MILLISECONDS, false));
    }

    public MetricsAPI() {
        this.systemMetrics = new SystemMetrics();
    }

    @GET
    @Timed
    @Path("system")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner= $action=metrics_read"})
    public String system() {
        return JsonUtil.toJson(this.systemMetrics.metrics());
    }

    @GET
    @Timed
    @Path("backend")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner= $action=metrics_read"})
    public String backend(@Context GraphManager manager) {
        Map<String, Map<String, Object>> results = InsertionOrderUtil.newMap();
        for (String graph : manager.graphs()) {
            Vortex g = manager.graph(graph);
            Map<String, Object> metrics = InsertionOrderUtil.newMap();
            metrics.put(BackendMetrics.BACKEND, g.backend());
            try {
                metrics.putAll(g.metadata(null, "metrics"));
            } catch (Throwable e) {
                metrics.put(BackendMetrics.EXCEPTION, e.toString());
                LOG.debug("Failed to get backend metrics", e);
            }
            results.put(graph, metrics);
        }
        return JsonUtil.toJson(results);
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner= $action=metrics_read"})
    public String all() {
        ServerReporter reporter = ServerReporter.instance();
        Map<String, Map<String, ? extends Metric>> result = new LinkedHashMap<>();
        result.put("gauges", reporter.gauges());
        result.put("counters", reporter.counters());
        result.put("histograms", reporter.histograms());
        result.put("meters", reporter.meters());
        result.put("timers", reporter.timers());
        return JsonUtil.toJson(result);
    }

    @GET
    @Timed
    @Path("gauges")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner= $action=metrics_read"})
    public String gauges() {
        ServerReporter reporter = ServerReporter.instance();
        return JsonUtil.toJson(reporter.gauges());
    }

    @GET
    @Timed
    @Path("counters")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner= $action=metrics_read"})
    public String counters() {
        ServerReporter reporter = ServerReporter.instance();
        return JsonUtil.toJson(reporter.counters());
    }

    @GET
    @Timed
    @Path("histograms")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner= $action=metrics_read"})
    public String histograms() {
        ServerReporter reporter = ServerReporter.instance();
        return JsonUtil.toJson(reporter.histograms());
    }

    @GET
    @Timed
    @Path("meters")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner= $action=metrics_read"})
    public String meters() {
        ServerReporter reporter = ServerReporter.instance();
        return JsonUtil.toJson(reporter.meters());
    }

    @GET
    @Timed
    @Path("timers")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner= $action=metrics_read"})
    public String timers() {
        ServerReporter reporter = ServerReporter.instance();
        return JsonUtil.toJson(reporter.timers());
    }
}
