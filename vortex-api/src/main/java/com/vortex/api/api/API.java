
package com.vortex.api.api;

import com.vortex.vortexdb.VortexException;
import com.vortex.vortexdb.Vortex;
import com.vortex.api.core.GraphManager;
import com.vortex.api.define.Checkable;
import com.vortex.api.metrics.MetricsUtil;
import com.vortex.api.server.RestServer;
import com.vortex.common.util.E;
import com.vortex.vortexdb.util.JsonUtil;
import com.vortex.common.util.Log;
import com.codahale.metrics.Meter;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;

import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.NotSupportedException;
import javax.ws.rs.core.MediaType;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

public class API {

    private static final Logger LOG = Log.logger(RestServer.class);

    public static final String CHARSET = "UTF-8";

    public static final String APPLICATION_JSON = MediaType.APPLICATION_JSON;
    public static final String APPLICATION_JSON_WITH_CHARSET =
                               APPLICATION_JSON + ";charset=" + CHARSET;
    public static final String JSON = MediaType.APPLICATION_JSON_TYPE
                                               .getSubtype();

    public static final String ACTION_APPEND = "append";
    public static final String ACTION_ELIMINATE = "eliminate";
    public static final String ACTION_CLEAR = "clear";

    private static final Meter succeedMeter =
                         MetricsUtil.registerMeter(API.class, "commit-succeed");
    private static final Meter illegalArgErrorMeter =
                         MetricsUtil.registerMeter(API.class, "illegal-arg");
    private static final Meter expectedErrorMeter =
                         MetricsUtil.registerMeter(API.class, "expected-error");
    private static final Meter unknownErrorMeter =
                         MetricsUtil.registerMeter(API.class, "unknown-error");

    public static Vortex graph(GraphManager manager, String graph) {
        Vortex g = manager.graph(graph);
        if (g == null) {
            throw new NotFoundException(String.format(
                      "Graph '%s' does not exist",  graph));
        }
        return g;
    }

    public static Vortex graph4admin(GraphManager manager, String graph) {
        return graph(manager, graph).graph();
    }

    public static <R> R commit(Vortex g, Callable<R> callable) {
        Consumer<Throwable> rollback = (error) -> {
            if (error != null) {
                LOG.error("Failed to commit", error);
            }
            try {
                g.tx().rollback();
            } catch (Throwable e) {
                LOG.error("Failed to rollback", e);
            }
        };

        try {
            R result = callable.call();
            g.tx().commit();
            succeedMeter.mark();
            return result;
        } catch (IllegalArgumentException | NotFoundException |
                 ForbiddenException e) {
            illegalArgErrorMeter.mark();
            rollback.accept(null);
            throw e;
        } catch (RuntimeException e) {
            expectedErrorMeter.mark();
            rollback.accept(e);
            throw e;
        } catch (Throwable e) {
            unknownErrorMeter.mark();
            rollback.accept(e);
            // TODO: throw the origin exception 'e'
            throw new VortexException("Failed to commit", e);
        }
    }

    public static void commit(Vortex g, Runnable runnable) {
        commit(g, () -> {
            runnable.run();
            return null;
        });
    }

    public static Object[] properties(Map<String, Object> properties) {
        Object[] list = new Object[properties.size() * 2];
        int i = 0;
        for (Map.Entry<String, Object> prop : properties.entrySet()) {
            list[i++] = prop.getKey();
            list[i++] = prop.getValue();
        }
        return list;
    }

    protected static void checkCreatingBody(Checkable body) {
        E.checkArgumentNotNull(body, "The request body can't be empty");
        body.checkCreate(false);
    }

    protected static void checkUpdatingBody(Checkable body) {
        E.checkArgumentNotNull(body, "The request body can't be empty");
        body.checkUpdate();
    }

    protected static void checkCreatingBody(
                          Collection<? extends Checkable> bodys) {
        E.checkArgumentNotNull(bodys, "The request body can't be empty");
        for (Checkable body : bodys) {
            E.checkArgument(body != null,
                            "The batch body can't contain null record");
            body.checkCreate(true);
        }
    }

    protected static void checkUpdatingBody(
                          Collection<? extends Checkable> bodys) {
        E.checkArgumentNotNull(bodys, "The request body can't be empty");
        for (Checkable body : bodys) {
            E.checkArgumentNotNull(body,
                                   "The batch body can't contain null record");
            body.checkUpdate();
        }
    }

    @SuppressWarnings("unchecked")
    protected static Map<String, Object> parseProperties(String properties) {
        if (properties == null || properties.isEmpty()) {
            return ImmutableMap.of();
        }

        Map<String, Object> props = null;
        try {
            props = JsonUtil.fromJson(properties, Map.class);
        } catch (Exception ignored) {}

        // If properties is the string "null", props will be null
        E.checkArgument(props != null,
                        "Invalid request with properties: %s", properties);
        return props;
    }

    public static boolean checkAndParseAction(String action) {
        E.checkArgumentNotNull(action, "The action param can't be empty");
        if (action.equals(ACTION_APPEND)) {
            return true;
        } else if (action.equals(ACTION_ELIMINATE)) {
            return false;
        } else {
            throw new NotSupportedException(
                      String.format("Not support action '%s'", action));
        }
    }
}
