

package com.vortex.api.api.filter;

import com.vortex.common.config.VortexConfig;
import com.vortex.api.config.ServerOptions;
import com.vortex.api.define.WorkLoad;
import com.vortex.common.util.Bytes;
import com.vortex.common.util.E;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.RateLimiter;

import javax.inject.Singleton;
import javax.ws.rs.ServiceUnavailableException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.ext.Provider;
import java.util.List;
import java.util.Set;

@Provider
@Singleton
@PreMatching
public class LoadDetectFilter implements ContainerRequestFilter {

    private static final Set<String> WHITE_API_LIST = ImmutableSet.of(
            "",
            "apis",
            "metrics",
            "versions"
    );

    // Call gc every 30+ seconds if memory is low and request frequently
    private static final RateLimiter GC_RATE_LIMITER =
                         RateLimiter.create(1.0 / 30);

    @Context
    private javax.inject.Provider<VortexConfig> configProvider;
    @Context
    private javax.inject.Provider<WorkLoad> loadProvider;

    @Override
    public void filter(ContainerRequestContext context) {
        if (LoadDetectFilter.isWhiteAPI(context)) {
            return;
        }

        VortexConfig config = this.configProvider.get();

        int maxWorkerThreads = config.get(ServerOptions.MAX_WORKER_THREADS);
        WorkLoad load = this.loadProvider.get();
        // There will be a thread doesn't work, dedicated to statistics
        if (load.incrementAndGet() >= maxWorkerThreads) {
            throw new ServiceUnavailableException(String.format(
                      "The server is too busy to process the request, " +
                      "you can config %s to adjust it or try again later",
                      ServerOptions.MAX_WORKER_THREADS.name()));
        }

        long minFreeMemory = config.get(ServerOptions.MIN_FREE_MEMORY);
        long allocatedMem = Runtime.getRuntime().totalMemory() -
                            Runtime.getRuntime().freeMemory();
        long presumableFreeMem = (Runtime.getRuntime().maxMemory() -
                                  allocatedMem) / Bytes.MB;
        if (presumableFreeMem < minFreeMemory) {
            gcIfNeeded();
            throw new ServiceUnavailableException(String.format(
                      "The server available memory %s(MB) is below than " +
                      "threshold %s(MB) and can't process the request, " +
                      "you can config %s to adjust it or try again later",
                      presumableFreeMem, minFreeMemory,
                      ServerOptions.MIN_FREE_MEMORY.name()));
        }
    }

    public static boolean isWhiteAPI(ContainerRequestContext context) {
        List<PathSegment> segments = context.getUriInfo().getPathSegments();
        E.checkArgument(segments.size() > 0, "Invalid request uri '%s'",
                        context.getUriInfo().getPath());
        String rootPath = segments.get(0).getPath();
        return WHITE_API_LIST.contains(rootPath);
    }

    private static void gcIfNeeded() {
        if (GC_RATE_LIMITER.tryAcquire(1)) {
            System.gc();
        }
    }
}
