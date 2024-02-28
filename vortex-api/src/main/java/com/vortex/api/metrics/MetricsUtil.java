
package com.vortex.api.metrics;

import com.codahale.metrics.*;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;

public class MetricsUtil {

    private static final MetricRegistry registry =
                                        MetricManager.INSTANCE.getRegistry();

    public static <T> Gauge<T> registerGauge(Class<?> clazz, String name,
                                             Gauge<T> gauge) {
        return registry.register(MetricRegistry.name(clazz, name), gauge);
    }

    public static Counter registerCounter(Class<?> clazz, String name) {
        return registry.counter(MetricRegistry.name(clazz, name));
    }

    public static Histogram registerHistogram(Class<?> clazz, String name) {
        return registry.histogram(MetricRegistry.name(clazz, name));
    }

    public static Meter registerMeter(Class<?> clazz, String name) {
        return registry.meter(MetricRegistry.name(clazz, name));
    }

    public static Timer registerTimer(Class<?> clazz, String name) {
        return registry.timer(MetricRegistry.name(clazz, name));
    }
}
