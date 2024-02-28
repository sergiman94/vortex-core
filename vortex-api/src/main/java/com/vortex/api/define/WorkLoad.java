
package com.vortex.api.define;

import java.util.concurrent.atomic.AtomicInteger;

public final class WorkLoad {

    private final AtomicInteger load;

    public WorkLoad() {
        this(0);
    }

    public WorkLoad(int load) {
        this.load = new AtomicInteger(load);
    }

    public WorkLoad(AtomicInteger load) {
        this.load = load;
    }

    public AtomicInteger get() {
        return this.load;
    }

    public int incrementAndGet() {
        return this.load.incrementAndGet();
    }

    public int decrementAndGet() {
        return this.load.decrementAndGet();
    }
}
