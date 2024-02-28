
package com.vortex.api.api.profile;

import com.vortex.api.api.API;
import com.vortex.api.version.ApiVersion;
import com.vortex.vortexdb.version.CoreVersion;
import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableMap;

import javax.annotation.security.PermitAll;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import java.util.Map;

@Path("versions")
@Singleton
public class VersionAPI extends API {

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @PermitAll
    public Object list() {
        Map<String, String> versions = ImmutableMap.of("version", "v1",
                                       "core", CoreVersion.VERSION.toString(),
                                       "gremlin", CoreVersion.GREMLIN_VERSION,
                                       "api", ApiVersion.VERSION.toString());
        return ImmutableMap.of("versions", versions);
    }
}
