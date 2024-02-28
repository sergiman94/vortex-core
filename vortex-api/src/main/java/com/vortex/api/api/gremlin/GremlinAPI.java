
package com.vortex.api.api.gremlin;

import com.vortex.api.api.API;
import com.vortex.api.api.filter.CompressInterceptor.Compress;
import com.vortex.common.config.VortexConfig;
import com.vortex.api.config.ServerOptions;
import com.vortex.vortexdb.exception.VortexGremlinException;
import com.vortex.api.metrics.MetricsUtil;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.util.Map;
import java.util.Set;

@Path("gremlin")
@Singleton
public class GremlinAPI extends API {

    private static final Histogram gremlinInputHistogram =
            MetricsUtil.registerHistogram(GremlinAPI.class, "gremlin-input");
    private static final Histogram gremlinOutputHistogram =
            MetricsUtil.registerHistogram(GremlinAPI.class, "gremlin-output");

    private static final Set<String> FORBIDDEN_REQUEST_EXCEPTIONS =
            ImmutableSet.of("java.lang.SecurityException",
                            "javax.ws.rs.ForbiddenException");
    private static final Set<String> BAD_REQUEST_EXCEPTIONS = ImmutableSet.of(
            "java.lang.IllegalArgumentException",
            "java.util.concurrent.TimeoutException",
            "groovy.lang.",
            "org.codehaus.",
            "com.vortex.vortexdb."
    );

    @Context
    private javax.inject.Provider<VortexConfig> configProvider;

    private GremlinClient client;

    public GremlinClient client() {
        if (this.client != null) {
            return this.client;
        }
        VortexConfig config = this.configProvider.get();
        String url = config.get(ServerOptions.GREMLIN_SERVER_URL);
        int timeout = config.get(ServerOptions.GREMLIN_SERVER_TIMEOUT) * 1000;
        int maxRoutes = config.get(ServerOptions.GREMLIN_SERVER_MAX_ROUTE);
        this.client = new GremlinClient(url, timeout, maxRoutes, maxRoutes);
        return this.client;
    }

    @POST
    @Timed
    @Compress
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Response post(@Context VortexConfig conf,
                         @Context HttpHeaders headers,
                         String request) {
        /* The following code is reserved for forwarding request */
        // context.getRequestDispatcher(location).forward(request, response);
        // return Response.seeOther(UriBuilder.fromUri(location).build())
        // .build();
        // Response.temporaryRedirect(UriBuilder.fromUri(location).build())
        // .build();
        String auth = headers.getHeaderString(HttpHeaders.AUTHORIZATION);
        Response response = this.client().doPostRequest(auth, request);
        gremlinInputHistogram.update(request.length());
        gremlinOutputHistogram.update(response.getLength());
        return transformResponseIfNeeded(response);
    }

    @GET
    @Timed
    @Compress(buffer=(1024 * 40))
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Response get(@Context VortexConfig conf,
                        @Context HttpHeaders headers,
                        @Context UriInfo uriInfo) {
        String auth = headers.getHeaderString(HttpHeaders.AUTHORIZATION);
        String query = uriInfo.getRequestUri().getRawQuery();
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();
        Response response = this.client().doGetRequest(auth, params);
        gremlinInputHistogram.update(query.length());
        gremlinOutputHistogram.update(response.getLength());
        return transformResponseIfNeeded(response);
    }

    private static Response transformResponseIfNeeded(Response response) {
        MediaType mediaType = response.getMediaType();
        if (mediaType != null) {
            // Append charset
            assert MediaType.APPLICATION_JSON_TYPE.equals(mediaType);
            response.getHeaders().putSingle(HttpHeaders.CONTENT_TYPE,
                                            mediaType.withCharset(CHARSET));
        }

        Response.StatusType status = response.getStatusInfo();
        if (status.getStatusCode() < 400) {
            // No need to transform if normal response without error
            return response;
        }

        if (mediaType == null || !JSON.equals(mediaType.getSubtype())) {
            String message = response.readEntity(String.class);
            throw new VortexGremlinException(status.getStatusCode(),
                                           ImmutableMap.of("message", message));
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> map = response.readEntity(Map.class);
        String exClassName = (String) map.get("Exception-Class");
        if (FORBIDDEN_REQUEST_EXCEPTIONS.contains(exClassName)) {
            status = Response.Status.FORBIDDEN;
        } else if (matchBadRequestException(exClassName)) {
            status = Response.Status.BAD_REQUEST;
        }
        throw new VortexGremlinException(status.getStatusCode(), map);
    }

    private static boolean matchBadRequestException(String exClass) {
        if (exClass == null) {
            return false;
        }
        if (BAD_REQUEST_EXCEPTIONS.contains(exClass)) {
            return true;
        }
        return BAD_REQUEST_EXCEPTIONS.stream().anyMatch(exClass::startsWith);
    }
}
