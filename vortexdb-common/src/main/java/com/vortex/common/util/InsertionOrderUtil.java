
package com.vortex.common.util;

import java.util.*;

public final class InsertionOrderUtil {

    public static <K, V> Map<K, V> newMap() {
        return new LinkedHashMap<>();
    }

    public static <K, V> Map<K, V> newMap(int initialCapacity) {
        return new LinkedHashMap<>(initialCapacity);
    }

    public static <K, V> Map<K, V> newMap(Map<K, V> origin) {
        return new LinkedHashMap<>(origin);
    }

    public static <V> Set<V> newSet() {
        return new LinkedHashSet<>();
    }

    public static <V> Set<V> newSet(int initialCapacity) {
        return new LinkedHashSet<>(initialCapacity);
    }

    public static <V> Set<V> newSet(Set<V> origin) {
        return new LinkedHashSet<>(origin);
    }

    public static <V> List<V> newList() {
        return new ArrayList<>();
    }

    public static <V> List<V> newList(int initialCapacity) {
        return new ArrayList<>(initialCapacity);
    }

    public static <V> List<V> newList(List<V> origin) {
        return new ArrayList<>(origin);
    }
}
