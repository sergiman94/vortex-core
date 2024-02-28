
package com.vortex.vortexdb.backend.store.ram;

import com.vortex.vortexdb.VortexException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

public final class IntLongMap implements RamMap {

    // TODO: use com.carrotsearch.hppc.IntLongHashMap instead
    private final long[] array;
    private int size;

    public IntLongMap(int capacity) {
        this.array = new long[capacity];
        this.size = 0;
    }

    public void put(int key, long value) {
        if (key >= this.size || key < 0) {
            throw new VortexException("Invalid key %s", key);
        }
        this.array[key] = value;
    }

    public int add(long value) {
        if (this.size == Integer.MAX_VALUE) {
            throw new VortexException("Too many edges %s", this.size);
        }
        int index = this.size;
        this.array[index] = value;
        this.size++;
        return index;
    }

    public long get(int key) {
        if (key >= this.size || key < 0) {
            throw new VortexException("Invalid key %s", key);
        }
        return this.array[key];
    }

    @Override
    public void clear() {
        Arrays.fill(this.array, 0L);
        this.size = 0;
    }

    @Override
    public long size() {
        return this.size;
    }

    @Override
    public void writeTo(DataOutputStream buffer) throws IOException {
        buffer.writeInt(this.array.length);
        for (long value : this.array) {
            buffer.writeLong(value);
        }
    }

    @Override
    public void readFrom(DataInputStream buffer) throws IOException {
        int size = buffer.readInt();
        if (size > this.array.length) {
            throw new VortexException("Invalid size %s, expect < %s",
                                    size, this.array.length);
        }
        for (int i = 0; i < size; i++) {
            long value = buffer.readLong();
            this.array[i] = value;
        }
        this.size = size;
    }
}
