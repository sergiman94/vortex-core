package com.vortex.common.event;

import com.google.common.collect.ImmutableList;
import com.vortex.common.iterator.ExtendableIterator;
import com.vortex.common.util.E;
import com.vortex.common.util.ExecutorUtil;
import com.vortex.common.util.Log;
import org.slf4j.Logger;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class EventHub {

    private static final Logger LOG = Log.logger(EventHub.class);

    public static final String EVENT_WORKER = "event-worker-%d";
    public static final String  ANY_EVENT = "*";

    private static final List<EventListener> EMPTY = ImmutableList.of();

    // Event executor
    private static ExecutorService executor = null;

    private String name;
    private Map<String, List<EventListener>> listeners;

    public EventHub() {this ("hub");}

    public EventHub(String name) {
        LOG.debug(("Create new EventHub:  {}"));

        this.name = name;
        this.listeners = new ConcurrentHashMap<>();

        EventHub.init(1);
    }

    public static synchronized void init (int poolSize) {
        if (executor != null) {
            return;
        }

        LOG.debug("Init pool(size {}) for EventHub", poolSize);
        executor = ExecutorUtil.newFixedThreadPool(poolSize, EVENT_WORKER);
    }

    public static synchronized  boolean destroy(long timeout) throws InterruptedException  {
        E.checkState(executor != null, "EventHub has not been initialized");
        LOG.debug("Destroy pool for EventHub");
        executor.shutdown();
        return executor.awaitTermination(timeout, TimeUnit.SECONDS);
    }

    private static ExecutorService executor() {
        ExecutorService exec = executor;
        E.checkState(exec != null, "The event executor has been destroyed");
        return exec;
    }

    public String name() {return this.name;}

    public boolean containsListener(String event) {
        List<EventListener> ls = this.listeners.get(event);
        return ls != null && ls.size() > 0;
    }

    public List<EventListener> listeners(String event) {
        List<EventListener> list = this.listeners.get(event);
        return list == null ? EMPTY : Collections.unmodifiableList(list);
    }

    public void listen (String event, EventListener listener) {
        E.checkNotNull(event, "event");
        E.checkNotNull(listener, "event listener");

        if (!this.listeners.containsKey(event)) {
            this.listeners.putIfAbsent(event, new CopyOnWriteArrayList<>());
        }

        List<EventListener> ls = this.listeners.get(event);
        assert  ls != null : this.listeners;
        ls.add(listener);
    }

    public List<EventListener> unlisten(String event) {
        List<EventListener> list = this.listeners.remove(event);
        return list == null ? EMPTY : Collections.unmodifiableList(list);
    }

    public int unlisten(String event, EventListener listener) {
        List<EventListener> ls = this.listeners.get(event);
        if (ls == null) {
            return 0;
        }

        int count = 0;
        while (ls.remove((listener))) {
            count ++;
        }

        return count;
    }

    public Future<Integer> notify(String event, @Nullable Object... args) {
        @SuppressWarnings("resource")
        ExtendableIterator<EventListener> all = new ExtendableIterator<>();

        List<EventListener> list = this.listeners.get(event);
        if(list != null && !list.isEmpty())
            all.extend(list.iterator());

        List<EventListener> listAny = this.listeners.get(ANY_EVENT);
        if (listAny != null && !listAny.isEmpty())
            all.extend(listAny.iterator());

        if (!all.hasNext())
            return CompletableFuture.completedFuture(0);

        Event ev = new Event(this, event, args);

        // The submit will catch params: ´all´(listeners) and ´ev´(event)
        return executor().submit(() -> {
            int count = 0;

            // Notify all listeners, and ignore the result
            while (all.hasNext()) {
                try {
                    all.next().event(ev);
                    count++;
                } catch (Throwable ignored) {
                    LOG.warn("Failed to handle event: {}", ev, ignored);
                }
            }

            return count;
        });
    }

    public Object call(String event, @Nullable Object... args) {
        List<EventListener> list = this.listeners.get(event);

        if (list == null) {
            throw new RuntimeException("Not found listener for: " + event);
        } else if (list.size() != 1) {
            throw  new RuntimeException("Too many listeners for: " +  event);
        }

        EventListener listener = list.get(0);
        return listener.event(new Event(this, event, args));
    }


}
