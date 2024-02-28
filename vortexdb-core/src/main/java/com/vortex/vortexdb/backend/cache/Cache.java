
package com.vortex.vortexdb.backend.cache;

import java.util.function.Consumer;
import java.util.function.Function;

public interface Cache<K, V> {

    public static final String ACTION_INVALID = "invalid";
    public static final String ACTION_CLEAR = "clear";
    public static final String ACTION_INVALIDED = "invalided";
    public static final String ACTION_CLEARED = "cleared";

    public V get(K id);

    public V getOrFetch(K id, Function<K, V> fetcher);

    public boolean containsKey(K id);

    public boolean update(K id, V value);

    public boolean update(K id, V value, long timeOffset);

    public boolean updateIfAbsent(K id, V value);

    public boolean updateIfPresent(K id, V value);

    public void invalidate(K id);

    public void traverse(Consumer<V> consumer);

    public void clear();

    public void expire(long ms);

    public long expire();

    public long tick();

    public long capacity();

    public long size();

    public boolean enableMetrics(boolean enabled);

    public long hits();

    public long miss();

    public <T> T attachment(T object);

    public <T> T attachment();
}
