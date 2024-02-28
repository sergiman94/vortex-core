
package com.vortex.common.concurrent;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {

    private static final LockManager INSTANCE = new LockManager();

    public static LockManager instance() {
        return INSTANCE;
    }

    private Map<String, LockGroup> lockGroupMap;

    private LockManager() {
        this.lockGroupMap = new ConcurrentHashMap<>();
    }

    public boolean exists(String group) {
        return this.lockGroupMap.containsKey(group);
    }

    public LockGroup create(String group) {
        if (exists(group)) {
            throw new RuntimeException(String.format(
                      "LockGroup '%s' already exists", group));
        }
        LockGroup lockGroup = new LockGroup(group);
        LockGroup previous = this.lockGroupMap.putIfAbsent(group, lockGroup);
        if (previous != null) {
            return previous;
        }
        return lockGroup;
    }

    public LockGroup get(String group) {
        LockGroup lockGroup = this.lockGroupMap.get(group);
        if (lockGroup == null) {
            throw new RuntimeException(String.format(
                      "LockGroup '%s' does not exists", group));
        }
        return lockGroup;
    }

    public void destroy(String group) {
        if (this.exists(group)) {
            this.lockGroupMap.remove(group);
        } else {
            throw new RuntimeException(String.format(
                      "LockGroup '%s' does not exists", group));
        }
    }
}
