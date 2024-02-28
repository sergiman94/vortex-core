
package com.vortex.common.concurrent;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LockGroup {

    private final String name;
    private final Map<String, Object> locksMap;

    public LockGroup(String lockGroup) {
        this.name = lockGroup;
        this.locksMap = new ConcurrentHashMap<>();
    }

    public Lock lock(String lockName) {
        if (!this.locksMap.containsKey(lockName)) {
            this.locksMap.putIfAbsent(lockName, new ReentrantLock());
        }
        return (Lock) this.locksMap.get(lockName);
    }

    public AtomicLock atomicLock(String lockName) {
        if (!this.locksMap.containsKey(lockName)) {
            this.locksMap.putIfAbsent(lockName, new AtomicLock(lockName));
        }
        return (AtomicLock) this.locksMap.get(lockName);
    }

    public ReadWriteLock readWriteLock(String lockName) {
        if (!this.locksMap.containsKey(lockName)) {
            this.locksMap.putIfAbsent(lockName, new ReentrantReadWriteLock());
        }
        return (ReadWriteLock) this.locksMap.get(lockName);
    }

    public KeyLock keyLock(String lockName) {
        if (!this.locksMap.containsKey(lockName)) {
            this.locksMap.putIfAbsent(lockName, new KeyLock());
        }
        return (KeyLock) this.locksMap.get(lockName);
    }

    public KeyLock keyLock(String lockName, int size) {
        if (!this.locksMap.containsKey(lockName)) {
            this.locksMap.putIfAbsent(lockName, new KeyLock(size));
        }
        return (KeyLock) this.locksMap.get(lockName);
    }

    public <K extends Comparable<K>> RowLock<K> rowLock(String lockName) {
        if (!this.locksMap.containsKey(lockName)) {
            this.locksMap.putIfAbsent(lockName, new RowLock<>());
        }
        Object value = this.locksMap.get(lockName);
        @SuppressWarnings("unchecked")
        RowLock<K> lock = (RowLock<K>) value;
        return lock;
    }

    public String name() {
        return this.name;
    }
}
