
package com.vortex.vortexdb.backend.cache;

import com.vortex.common.util.Log;
import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.common.util.E;
import com.vortex.common.util.Log;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

public class CacheManager {

    private static final Logger LOG = Log.logger(Cache.class);

    private static CacheManager INSTANCE = new CacheManager();

    // Check the cache expiration every 30s by default
    private static final long TIMER_TICK_PERIOD = 30;
    // Log if tick cost time > 1000ms
    private static final long LOG_TICK_COST_TIME = 1000L;

    private final Map<String, Cache<Id, ?>> caches;
    private final Timer timer;

    public static CacheManager instance() {
        return INSTANCE;
    }

    public static boolean cacheEnableMetrics(String name, boolean enabled) {
        Cache<Id, ?> cache = INSTANCE.caches.get(name);
        E.checkArgument(cache != null,
                        "Not found cache named '%s'", name);
        return cache.enableMetrics(enabled);
    }

    public CacheManager() {
        this.caches = new ConcurrentHashMap<>();
        this.timer = new Timer("cache-expirer", true);

        this.scheduleTimer(TIMER_TICK_PERIOD);
    }

    private TimerTask scheduleTimer(float period) {
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                try {
                    for (Entry<String, Cache<Id, Object>> entry :
                         caches().entrySet()) {
                        this.tick(entry.getKey(), entry.getValue());
                    }
                } catch (Throwable e) {
                    LOG.warn("An exception occurred when running tick", e);
                }
            }

            private void tick(String name, Cache<Id, Object> cache) {
                long start = System.currentTimeMillis();
                long items = cache.tick();
                long cost = System.currentTimeMillis() - start;
                if (cost > LOG_TICK_COST_TIME) {
                    LOG.info("Cache '{}' expired {} items cost {}ms > {}ms " +
                             "(size {}, expire {}ms)", name, items, cost,
                             LOG_TICK_COST_TIME, cache.size(), cache.expire());
                }
                LOG.debug("Cache '{}' expiration tick cost {}ms", name, cost);
            }
        };

        // Schedule task with the period in seconds
        this.timer.schedule(task, 0, (long) (period * 1000.0));

        return task;
    }

    public <V> Map<String, Cache<Id, V>> caches() {
        @SuppressWarnings({ "rawtypes", "unchecked" })
        Map<String, Cache<Id, V>> caches = (Map) this.caches;
        return Collections.unmodifiableMap(caches);
    }

    public <V> Cache<Id, V> cache(String name) {
        return this.cache(name, RamCache.DEFAULT_SIZE);
    }

    public <V> Cache<Id, V> cache(String name, long capacity) {
        if (!this.caches.containsKey(name)) {
            this.caches.putIfAbsent(name, new RamCache(capacity));
            LOG.info("Init RamCache for '{}' with capacity {}",
                     name, capacity);
        }
        @SuppressWarnings("unchecked")
        Cache<Id, V> cache = (Cache<Id, V>) this.caches.get(name);
        E.checkArgument(cache instanceof RamCache,
                        "Invalid cache implement: %s", cache.getClass());
        return cache;
    }

    public <V> Cache<Id, V> offheapCache(Vortex graph, String name,
                                         long capacity, long avgElemSize) {
        if (!this.caches.containsKey(name)) {
            OffheapCache cache = new OffheapCache(graph, capacity, avgElemSize);
            this.caches.putIfAbsent(name, cache);
            LOG.info("Init OffheapCache for '{}' with capacity {}",
                     name, capacity);
        }
        @SuppressWarnings("unchecked")
        Cache<Id, V> cache = (Cache<Id, V>) this.caches.get(name);
        E.checkArgument(cache instanceof OffheapCache,
                        "Invalid cache implement: %s", cache.getClass());
        return cache;
    }

    public <V> Cache<Id, V> levelCache(Vortex graph, String name,
                                       long capacity1, long capacity2,
                                       long avgElemSize) {
        if (!this.caches.containsKey(name)) {
            RamCache cache1 = new RamCache(capacity1);
            OffheapCache cache2 = new OffheapCache(graph, capacity2,
                                                   avgElemSize);
            this.caches.putIfAbsent(name, new LevelCache(cache1, cache2));
            LOG.info("Init LevelCache for '{}' with capacity {}:{}",
                     name, capacity1, capacity2);
        }
        @SuppressWarnings("unchecked")
        Cache<Id, V> cache = (Cache<Id, V>) this.caches.get(name);
        E.checkArgument(cache instanceof LevelCache,
                        "Invalid cache implement: %s", cache.getClass());
        return cache;
    }
}
