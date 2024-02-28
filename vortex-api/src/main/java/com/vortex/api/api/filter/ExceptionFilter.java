
package com.vortex.api.api.filter;

import com.vortex.vortexdb.VortexException;
import com.vortex.api.api.API;
import com.vortex.common.config.VortexConfig;
import com.vortex.api.config.ServerOptions;
import com.vortex.vortexdb.exception.VortexGremlinException;
import com.vortex.vortexdb.exception.NotFoundException;
import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.hk2.api.MultiException;

import javax.annotation.security.RolesAllowed;
import javax.inject.Singleton;
import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObjectBuilder;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class ExceptionFilter {

    private static final int BAD_REQUEST_ERROR =
            Response.Status.BAD_REQUEST.getStatusCode();
    private static final int NOT_FOUND_ERROR =
            Response.Status.NOT_FOUND.getStatusCode();
    private static final int INTERNAL_SERVER_ERROR =
            Response.Status.INTERNAL_SERVER_ERROR.getStatusCode();

    public static class TracedExceptionMapper extends API {

        private static boolean forcedTrace = false;

        @Context
        private javax.inject.Provider<VortexConfig> configProvider;

        protected boolean trace() {
            if (forcedTrace) {
                return true;
            }
            VortexConfig config = this.configProvider.get();
            if (config == null) {
                return false;
            }
            return config.get(ServerOptions.ALLOW_TRACE);
        }
    }

    @Path("exception/trace")
    @Singleton
    public static class TracedExceptionAPI extends API {

        @GET
        @Timed
        @Produces(APPLICATION_JSON_WITH_CHARSET)
        @RolesAllowed({"admin"})
        public Object get() {
            return ImmutableMap.of("trace", TracedExceptionMapper.forcedTrace);
        }

        @PUT
        @Timed
        @Consumes(APPLICATION_JSON)
        @Produces(APPLICATION_JSON_WITH_CHARSET)
        @RolesAllowed({"admin"})
        public Object trace(boolean trace) {
            TracedExceptionMapper.forcedTrace = trace;
            return ImmutableMap.of("trace", TracedExceptionMapper.forcedTrace);
        }
    }

    @Provider
    public static class VortexExceptionMapper
                  extends TracedExceptionMapper
                  implements ExceptionMapper<VortexException> {

        @Override
        public Response toResponse(VortexException exception) {
            return Response.status(BAD_REQUEST_ERROR)
                           .type(MediaType.APPLICATION_JSON)
                           .entity(formatException(exception, this.trace()))
                           .build();
        }
    }

    @Provider
    public static class IllegalArgumentExceptionMapper
                  extends TracedExceptionMapper
                  implements ExceptionMapper<IllegalArgumentException> {

        @Override
        public Response toResponse(IllegalArgumentException exception) {
            return Response.status(BAD_REQUEST_ERROR)
                           .type(MediaType.APPLICATION_JSON)
                           .entity(formatException(exception, this.trace()))
                           .build();
        }
    }

    @Provider
    public static class NotFoundExceptionMapper
                  extends TracedExceptionMapper
                  implements ExceptionMapper<NotFoundException> {

        @Override
        public Response toResponse(NotFoundException exception) {
            return Response.status(NOT_FOUND_ERROR)
                           .type(MediaType.APPLICATION_JSON)
                           .entity(formatException(exception, this.trace()))
                           .build();
        }
    }

    @Provider
    public static class NoSuchElementExceptionMapper
                  extends TracedExceptionMapper
                  implements ExceptionMapper<NoSuchElementException> {

        @Override
        public Response toResponse(NoSuchElementException exception) {
            return Response.status(NOT_FOUND_ERROR)
                           .type(MediaType.APPLICATION_JSON)
                           .entity(formatException(exception, this.trace()))
                           .build();
        }
    }

    @Provider
    public static class WebApplicationExceptionMapper
                  extends TracedExceptionMapper
                  implements ExceptionMapper<WebApplicationException> {

        @Override
        public Response toResponse(WebApplicationException exception) {
            Response response = exception.getResponse();
            if (response.hasEntity()) {
                return response;
            }
            MultivaluedMap<String, Object> headers = response.getHeaders();
            boolean trace = this.trace(response.getStatus());
            response = Response.status(response.getStatus())
                               .type(MediaType.APPLICATION_JSON)
                               .entity(formatException(exception, trace))
                               .build();
            response.getHeaders().putAll(headers);
            return response;
        }

        private boolean trace(int status) {
            return this.trace() && status == INTERNAL_SERVER_ERROR;
        }
    }

    @Provider
    public static class VortexGremlinExceptionMapper
                  extends TracedExceptionMapper
                  implements ExceptionMapper<VortexGremlinException> {

        @Override
        public Response toResponse(VortexGremlinException exception) {
            return Response.status(exception.statusCode())
                           .type(MediaType.APPLICATION_JSON)
                           .entity(formatGremlinException(exception,
                                                          this.trace()))
                           .build();
        }
    }

    @Provider
    public static class AssertionErrorMapper extends TracedExceptionMapper
                  implements ExceptionMapper<AssertionError> {

        @Override
        public Response toResponse(AssertionError exception) {
            return Response.status(INTERNAL_SERVER_ERROR)
                           .type(MediaType.APPLICATION_JSON)
                           .entity(formatException(exception, true))
                           .build();
        }
    }

    @Provider
    public static class UnknownExceptionMapper extends TracedExceptionMapper
                  implements ExceptionMapper<Throwable> {

        @Override
        public Response toResponse(Throwable exception) {
            if (exception instanceof MultiException &&
                ((MultiException) exception).getErrors().size() == 1) {
                exception = ((MultiException) exception).getErrors().get(0);
            }
            return Response.status(INTERNAL_SERVER_ERROR)
                           .type(MediaType.APPLICATION_JSON)
                           .entity(formatException(exception, this.trace()))
                           .build();
        }
    }

    public static String formatException(Throwable exception, boolean trace) {
        String clazz = exception.getClass().toString();
        String message = exception.getMessage() != null ?
                         exception.getMessage() : "";
        String cause = exception.getCause() != null ?
                       exception.getCause().toString() : "";

        JsonObjectBuilder json = Json.createObjectBuilder()
                                     .add("exception", clazz)
                                     .add("message", message)
                                     .add("cause", cause);

        if (trace) {
            JsonArrayBuilder traces = Json.createArrayBuilder();
            for (StackTraceElement i : exception.getStackTrace()) {
                traces.add(i.toString());
            }
            json.add("trace", traces);
        }

        return json.build().toString();
    }

    public static String formatGremlinException(VortexGremlinException exception,
                                                boolean trace) {
        Map<String, Object> map = exception.response();
        String message = (String) map.get("message");
        String exClassName = (String) map.get("Exception-Class");
        @SuppressWarnings("unchecked")
        List<String> exceptions = (List<String>) map.get("exceptions");
        String stackTrace = (String) map.get("stackTrace");

        message = message != null ? message : "";
        exClassName = exClassName != null ? exClassName : "";
        String cause = exceptions != null ? exceptions.toString() : "";

        JsonObjectBuilder json = Json.createObjectBuilder()
                                     .add("exception", exClassName)
                                     .add("message", message)
                                     .add("cause", cause);

        if (trace && stackTrace != null) {
            JsonArrayBuilder traces = Json.createArrayBuilder();
            for (String part : StringUtils.split(stackTrace, '\n')) {
                traces.add(part);
            }
            json.add("trace", traces);
        }

        return json.build().toString();
    }
}
