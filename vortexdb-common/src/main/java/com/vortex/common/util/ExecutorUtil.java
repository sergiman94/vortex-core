package com.vortex.common.util;

import com.vortex.common.concurrent.PausableScheduledThreadPool;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

public class ExecutorUtil {

    public static ExecutorService newFixedThreadPool(String name) {return newFixedThreadPool(1, name);}

    public static ExecutorService newFixedThreadPool (int size, String name) {
        ThreadFactory factory = new BasicThreadFactory.Builder().namingPattern(name).build();

        return Executors.newFixedThreadPool(size, factory);
    }

    public static ScheduledExecutorService newScheduledThreadPool(String name) {
        return newScheduledThreadPool(1, name);
    }

    public static ScheduledExecutorService newScheduledThreadPool(int size, String name) {
        ThreadFactory factory = new BasicThreadFactory.Builder().namingPattern(name).build();
        return Executors.newScheduledThreadPool(size, factory);
    }

    public static PausableScheduledThreadPool newPausableScheduledThreadPool(String name) {
        return newPausableScheduledThreadPool(1, name);
    }

    public static PausableScheduledThreadPool newPausableScheduledThreadPool(int size, String name) {
        ThreadFactory factory =  new BasicThreadFactory.Builder().namingPattern(name).build();

        return new PausableScheduledThreadPool(size, factory);
    }

}
