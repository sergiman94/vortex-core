
package com.vortex.api.api.gremlin;

import com.vortex.api.api.filter.CompressInterceptor;
import com.vortex.common.rest.AbstractRestClient;
import com.vortex.common.testutil.Whitebox;
import com.vortex.common.util.E;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;

public class GremlinClient extends AbstractRestClient {

    private final WebTarget webTarget;

    public GremlinClient(String url, int timeout,
                         int maxTotal, int maxPerRoute) {
        super(url, timeout, maxTotal, maxPerRoute);
        this.webTarget = Whitebox.getInternalState(this, "target");
        E.checkNotNull(this.webTarget, "target");
    }

    @Override
    protected void checkStatus(javax.ws.rs.core.Response response, javax.ws.rs.core.Response.Status... statuses) {
        
    }

//    @Override
//    protected void checkStatus(Response response, Response.Status... statuses) {
//
//    }


    public Response doPostRequest(String auth, String req) {
        Entity<?> body = Entity.entity(req, MediaType.APPLICATION_JSON);
        return this.webTarget.request()
                             .header(HttpHeaders.AUTHORIZATION, auth)
                             .accept(MediaType.APPLICATION_JSON)
                             .acceptEncoding(CompressInterceptor.GZIP)
                             .post(body);
    }

    public Response doGetRequest(String auth,
                                 MultivaluedMap<String, String> params) {
        WebTarget target = this.webTarget;
        for (Map.Entry<String, List<String>> entry : params.entrySet()) {
            E.checkArgument(entry.getValue().size() == 1,
                            "Invalid query param '%s', can only accept " +
                            "one value, but got %s",
                            entry.getKey(), entry.getValue());
            target = target.queryParam(entry.getKey(), entry.getValue().get(0));
        }
        return target.request()
                     .header(HttpHeaders.AUTHORIZATION, auth)
                     .accept(MediaType.APPLICATION_JSON)
                     .acceptEncoding(CompressInterceptor.GZIP)
                     .get();
    }
}
