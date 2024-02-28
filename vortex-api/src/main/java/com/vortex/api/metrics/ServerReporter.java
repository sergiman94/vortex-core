
package com.vortex.api.metrics;

import com.vortex.common.util.E;
import com.codahale.metrics.*;
import com.google.common.collect.ImmutableSortedMap;

import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ServerReporter extends ScheduledReporter {

    private static volatile ServerReporter instance = null;

    private SortedMap<String, Gauge<?>> gauges;
    private SortedMap<String, Counter> counters;
    private SortedMap<String, Histogram> histograms;
    private SortedMap<String, Meter> meters;
    private SortedMap<String, Timer> timers;

    public static synchronized ServerReporter instance(
                                              MetricRegistry registry) {
        if (instance == null) {
            synchronized (ServerReporter.class) {
                if (instance == null) {
                    instance = new ServerReporter(registry);
                }
            }
        }
        return instance;
    }

    public static ServerReporter instance() {
        E.checkNotNull(instance, "Must instantiate ServerReporter before get");
        return instance;
    }

    private ServerReporter(MetricRegistry registry) {
        this(registry, SECONDS, MILLISECONDS, MetricFilter.ALL);
    }

    private ServerReporter(MetricRegistry registry, TimeUnit rateUnit,
                           TimeUnit durationUnit, MetricFilter filter) {
        super(registry, "server-reporter", filter, rateUnit, durationUnit);
        this.gauges = ImmutableSortedMap.of();
        this.counters = ImmutableSortedMap.of();
        this.histograms = ImmutableSortedMap.of();
        this.meters = ImmutableSortedMap.of();
        this.timers = ImmutableSortedMap.of();
    }

    public Map<String, Timer> timers() {
        return Collections.unmodifiableMap(this.timers);
    }

    public Map<String, Gauge<?>> gauges() {
        return Collections.unmodifiableMap(this.gauges);
    }

    public Map<String, Counter> counters() {
        return Collections.unmodifiableMap(this.counters);
    }

    public Map<String, Histogram> histograms() {
        return Collections.unmodifiableMap(this.histograms);
    }

    public Map<String, Meter> meters() {
        return Collections.unmodifiableMap(this.meters);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {
        this.gauges = (SortedMap) gauges;
        this.counters = counters;
        this.histograms = histograms;
        this.meters = meters;
        this.timers = timers;
    }
}
