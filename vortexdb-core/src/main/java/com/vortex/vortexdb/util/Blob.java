package com.vortex.vortexdb.util;

import com.vortex.common.util.Bytes;
import com.vortex.common.util.E;

import java.util.Arrays;

public final class Blob implements Comparable<Blob> {
    public static final Blob EMPTY = new Blob(new byte[0]);

    private final byte[] bytes;

    private Blob(byte[] bytes) {
        E.checkNotNull(bytes, "bytes");
        this.bytes = bytes;
    }

    public byte[] bytes() {
        return this.bytes;
    }

    public static Blob wrap(byte[] bytes) {
        return new Blob(bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(this.bytes);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Blob)) {
            return false;
        }
        Blob other = (Blob) obj;
        return Arrays.equals(this.bytes, other.bytes);
    }

    @Override
    public String toString() {
        String hex = Bytes.toHex(this.bytes);
        StringBuilder sb = new StringBuilder(6 + hex.length());
        sb.append("Blob{").append(hex).append("}");
        return sb.toString();
    }

    @Override
    public int compareTo(Blob other) {
        E.checkNotNull(other, "other blob");
        return Bytes.compare(this.bytes, other.bytes);
    }
}
