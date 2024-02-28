
package com.vortex.vortexdb.job.computer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ComputerPool {

    private static final ComputerPool INSTANCE = new ComputerPool();

    static {
        INSTANCE.register(new PageRankComputer());
        INSTANCE.register(new WeakConnectedComponentComputer());
        INSTANCE.register(new LpaComputer());
        INSTANCE.register(new TriangleCountComputer());
        INSTANCE.register(new LouvainComputer());
    }

    private final Map<String, Computer> computers;

    public ComputerPool() {
        this.computers = new ConcurrentHashMap<>();
    }

    public Computer register(Computer computer) {
        assert !this.computers.containsKey(computer.name());
        return this.computers.put(computer.name(), computer);
    }

    public Computer find(String name) {
        return this.computers.get(name);
    }

    public static ComputerPool instance() {
        return INSTANCE;
    }
}
