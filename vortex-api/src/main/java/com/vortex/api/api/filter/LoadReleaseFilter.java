
package com.vortex.api.api.filter;

import com.vortex.api.define.WorkLoad;

import javax.inject.Singleton;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.Context;
import javax.ws.rs.ext.Provider;

@Provider
@Singleton
public class LoadReleaseFilter implements ContainerResponseFilter {

    @Context
    private javax.inject.Provider<WorkLoad> loadProvider;

    @Override
    public void filter(ContainerRequestContext requestContext,
                       ContainerResponseContext responseContext) {
        if (LoadDetectFilter.isWhiteAPI(requestContext)) {
            return;
        }

        WorkLoad load = this.loadProvider.get();
        load.decrementAndGet();
    }
}
