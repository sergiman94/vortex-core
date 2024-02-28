
package com.vortex.vortexdb.backend.store.ram;

import com.vortex.vortexdb.exception.NotSupportException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

public final class IntObjectMap<V> implements RamMap {

    private final Object[] array;

    public IntObjectMap(int size) {
        this.array = new Object[size];
    }

    @SuppressWarnings("unchecked")
    public V get(int key) {
        return (V) this.array[key];
    }

    public void set(int key, V value) {
        this.array[key] = value;
    }

    @Override
    public void clear() {
        Arrays.fill(this.array, null);
    }

    @Override
    public long size() {
        return this.array.length;
    }

    @Override
    public void writeTo(DataOutputStream buffer) throws IOException {
        throw new NotSupportException("IntObjectMap.writeTo");
    }

    @Override
    public void readFrom(DataInputStream buffer) throws IOException {
        throw new NotSupportException("IntObjectMap.readFrom");
    }
}
