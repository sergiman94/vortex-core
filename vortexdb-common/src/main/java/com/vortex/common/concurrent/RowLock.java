
package com.vortex.common.concurrent;

import com.vortex.common.util.E;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RowLock<K extends Comparable<K>> {

    private final Map<K, Lock> locks = new ConcurrentHashMap<>();
    private final ThreadLocal<Map<K, LocalLock>> localLocks =
                  ThreadLocal.withInitial(HashMap::new);

    public void lock(K key) {
        E.checkArgument(key != null, "Lock key can't be null");
        LocalLock localLock = this.localLocks.get().get(key);
        if (localLock != null) {
            localLock.lockCount++;
        } else {
            Lock current = new ReentrantLock();
            Lock previous = this.locks.putIfAbsent(key, current);
            if (previous != null) {
                current = previous;
            }
            current.lock();
            this.localLocks.get().put(key, new LocalLock(current));
        }
    }

    public void unlock(K key) {
        E.checkArgument(key != null, "Unlock key can't be null");
        LocalLock localLock = this.localLocks.get().get(key);
        if (localLock == null) {
            return;
        }
        if (--localLock.lockCount == 0) {
            this.locks.remove(key, localLock.current);
            this.localLocks.get().remove(key);
            localLock.current.unlock();
        }
        E.checkState(localLock.lockCount >= 0,
                     "The lock count must be >= 0, but got %s",
                     localLock.lockCount);
    }

    public void lockAll(Set<K> keys) {
        E.checkArgument(keys != null && keys.size() > 0,
                        "Lock keys can't be null or empty");
        List<K> list = new ArrayList<>(keys);
        Collections.sort(list);
        for (K key : list) {
            this.lock(key);
        }
    }

    public void unlockAll(Set<K> keys) {
        E.checkArgument(keys != null && keys.size() > 0,
                        "Unlock keys can't be null or empty");
        for (K key : keys) {
            this.unlock(key);
        }
    }

    private static class LocalLock {

        private final Lock current;
        private int lockCount;

        private LocalLock(Lock current) {
            this.current = current;
            this.lockCount = 1;
        }
    }
}
