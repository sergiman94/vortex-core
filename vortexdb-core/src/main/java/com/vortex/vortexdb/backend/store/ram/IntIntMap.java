
package com.vortex.vortexdb.backend.store.ram;

import com.vortex.vortexdb.VortexException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

public final class IntIntMap implements RamMap {

    // TODO: use com.carrotsearch.hppc.IntIntHashMap instead
    private final int[] array;

    public IntIntMap(int capacity) {
        this.array = new int[capacity];
    }

    public void put(long key, int value) {
        assert 0 <= key && key < Integer.MAX_VALUE;
        this.array[(int) key] = value;
    }

    public int get(long key) {
        assert 0 <= key && key < Integer.MAX_VALUE;
        return this.array[(int) key];
    }

    @Override
    public void clear() {
        Arrays.fill(this.array, 0);
    }

    @Override
    public long size() {
        return this.array.length;
    }

    @Override
    public void writeTo(DataOutputStream buffer) throws IOException {
        buffer.writeInt(this.array.length);
        for (int value : this.array) {
            buffer.writeInt(value);
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
            int value = buffer.readInt();
            this.array[i] = value;
        }
    }
}
