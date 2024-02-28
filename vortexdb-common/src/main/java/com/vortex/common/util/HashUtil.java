
package com.vortex.common.util;

import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;

import java.nio.charset.Charset;

public final class HashUtil {

    private static final Charset CHARSET = Charsets.UTF_8;

    public static byte[] hash(byte[] bytes) {
        return Hashing.murmur3_32().hashBytes(bytes).asBytes();
    }

    public static String hash(String value) {
        return Hashing.murmur3_32().hashString(value, CHARSET).toString();
    }

    public static byte[] hash128(byte[] bytes) {
        return Hashing.murmur3_128().hashBytes(bytes).asBytes();
    }

    public static String hash128(String value) {
        return Hashing.murmur3_128().hashString(value, CHARSET).toString();
    }
}
