
package com.vortex.api.server;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.api.MultiException;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.process.internal.RequestScoped;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature;
import org.glassfish.jersey.server.monitoring.ApplicationEvent;
import org.glassfish.jersey.server.monitoring.ApplicationEventListener;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.server.monitoring.RequestEventListener;
import com.vortex.vortexdb.VortexException;
import com.vortex.common.config.VortexConfig;
import com.vortex.api.core.GraphManager;
import com.vortex.api.define.WorkLoad;
import com.vortex.common.event.EventHub;
import com.vortex.common.util.E;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jersey2.InstrumentedResourceMethodApplicationListener;


import javax.ws.rs.ApplicationPath;

@ApplicationPath("/")
public class ApplicationConfig extends ResourceConfig {

    public ApplicationConfig(VortexConfig conf, EventHub hub) {
        packages("com.vortex.api.api");

        // Register Jackson to support json
        register(org.glassfish.jersey.jackson.JacksonFeature.class);

        // Register to use the jsr250 annotations @RolesAllowed
        register(RolesAllowedDynamicFeature.class);

        // Register VortexConfig to context
        register(new ConfFactory(conf));

        // Register GraphManager to context
        register(new GraphManagerFactory(conf, hub));

        // Register WorkLoad to context
        register(new WorkLoadFactory());

        // Let @Metric annotations work
        MetricRegistry registry = MetricManager.INSTANCE.getRegistry();
        register(new InstrumentedResourceMethodApplicationListener(registry));
    }

    private class ConfFactory extends AbstractBinder
                              implements Factory<VortexConfig> {

        private VortexConfig conf = null;

        public ConfFactory(VortexConfig conf) {
            E.checkNotNull(conf, "configuration");
            this.conf = conf;
        }

        @Override
        protected void configure() {
            bindFactory(this).to(VortexConfig.class).in(RequestScoped.class);
        }

        @Override
        public VortexConfig provide() {
            return this.conf;
        }

        @Override
        public void dispose(VortexConfig conf) {
            // pass
        }
    }

    private class GraphManagerFactory extends AbstractBinder
                                      implements Factory<GraphManager> {

        private GraphManager manager = null;

        public GraphManagerFactory(VortexConfig conf, EventHub hub) {
            register(new ApplicationEventListener() {
                private final ApplicationEvent.Type EVENT_INITED =
                              ApplicationEvent.Type.INITIALIZATION_FINISHED;
                private final ApplicationEvent.Type EVENT_DESTROYED =
                              ApplicationEvent.Type.DESTROY_FINISHED;

                @Override
                public void onEvent(ApplicationEvent event) {
                    if (event.getType() == this.EVENT_INITED) {
                        GraphManagerFactory.this.manager = new GraphManager(conf, hub);
                    } else if (event.getType() == this.EVENT_DESTROYED) {
                        if (GraphManagerFactory.this.manager != null) {
                            GraphManagerFactory.this.manager.close();
                        }
                    }
                }

                @Override
                public RequestEventListener onRequest(RequestEvent event) {
                    return null;
                }
            });
        }

        @Override
        protected void configure() {
            bindFactory(this).to(GraphManager.class).in(RequestScoped.class);
        }

        @Override
        public GraphManager provide() {
            if (this.manager == null) {
                String message = "Please wait for the server to initialize";
                throw new MultiException(new VortexException(message), false);
            }
            return this.manager;
        }

        @Override
        public void dispose(GraphManager manager) {
            // pass
        }
    }

    private class WorkLoadFactory extends AbstractBinder
                                  implements Factory<WorkLoad> {

        private final WorkLoad load;

        public WorkLoadFactory() {
            this.load = new WorkLoad();
        }

        @Override
        public WorkLoad provide() {
            return this.load;
        }

        @Override
        public void dispose(WorkLoad workLoad) {
            // pass
        }

        @Override
        protected void configure() {
            bindFactory(this).to(WorkLoad.class).in(RequestScoped.class);
        }
    }
}
