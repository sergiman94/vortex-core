
package com.vortex.vortexdb.backend;

import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.id.IdGenerator;
import com.vortex.vortexdb.type.VortexType;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class LocalCounter {

    private final Map<VortexType, AtomicLong> counters;

    public LocalCounter() {
        this.counters = new ConcurrentHashMap<>();
    }

    public synchronized Id nextId(VortexType type) {
        AtomicLong counter = this.counters.get(type);
        if (counter == null) {
            counter = new AtomicLong(0);
            AtomicLong previous = this.counters.putIfAbsent(type, counter);
            if (previous != null) {
                counter = previous;
            }
        }
        return IdGenerator.of(counter.incrementAndGet());
    }

    public long getCounter(VortexType type) {
        AtomicLong counter = this.counters.get(type);
        if (counter == null) {
            counter = new AtomicLong(0);
            AtomicLong previous = this.counters.putIfAbsent(type, counter);
            if (previous != null) {
                counter = previous;
            }
        }
        return counter.longValue();
    }

    public synchronized void increaseCounter(VortexType type, long increment) {
        AtomicLong counter = this.counters.get(type);
        if (counter == null) {
            counter = new AtomicLong(0);
            AtomicLong previous = this.counters.putIfAbsent(type, counter);
            if (previous != null) {
                counter = previous;
            }
        }
        long oldValue = counter.longValue();
        AtomicLong value = new AtomicLong(oldValue + increment);
        this.counters.put(type, value);
    }

    public void reset() {
        this.counters.clear();
    }
}
