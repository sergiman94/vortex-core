
package com.vortex.vortexdb.util.collection;

import com.vortex.vortexdb.VortexException;
import com.vortex.common.perf.PerfUtil.Watched;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

public class ObjectIntMappingFactory {

    public static <V> ObjectIntMapping<V> newObjectIntMapping() {
        return newObjectIntMapping(false);
    }

    public static <V> ObjectIntMapping<V> newObjectIntMapping(
                                          boolean concurrent) {
        return concurrent ? new ConcurrentObjectIntMapping<>() :
                            new SingleThreadObjectIntMapping<>();
    }

    public static final class SingleThreadObjectIntMapping<V>
                        implements ObjectIntMapping<V> {

        private static final int MAGIC = 1 << 16;
        private static final int MAX_OFFSET = 10;

        private final IntObjectHashMap<V> int2IdMap;

        public SingleThreadObjectIntMapping() {
            this.int2IdMap = new IntObjectHashMap<>();
        }

        @Watched
        @SuppressWarnings("unchecked")
        @Override
        public int object2Code(Object object) {
            int code = object.hashCode();
            // TODO: improve hash algorithm
            for (int i = 1; i > 0; i <<= 1) {
                for (int j = 0; j < MAX_OFFSET; j++) {
                    if (code <= 0) {
                        if (code == 0) {
                            code = 1;
                        } else {
                            code = -code;
                        }
                    }
                    assert code > 0;
                    V existed = this.int2IdMap.get(code);
                    if (existed == null) {
                        this.int2IdMap.put(code, (V) object);
                        return code;
                    }
                    if (existed.equals(object)) {
                        return code;
                    }
                    code = code + i + j;
                    /*
                     * If i < MAGIC, try (i * 2) to reduce conflicts, otherwise
                     * try (i + 1), (i + 2), ..., (i + 10) to try more times
                     * before try (i * 2).
                     */
                    if (i < MAGIC) {
                        break;
                    }
                }
            }
            throw new VortexException("Failed to get code for object: %s", object);
        }

        @Watched
        @Override
        public V code2Object(int code) {
            assert code > 0;
            return this.int2IdMap.get(code);
        }

        @Override
        public void clear() {
            this.int2IdMap.clear();
        }

        @Override
        public String toString() {
            return this.int2IdMap.toString();
        }
    }

    public static final class ConcurrentObjectIntMapping<V>
                        implements ObjectIntMapping<V> {

        private final SingleThreadObjectIntMapping<V> objectIntMapping;

        public ConcurrentObjectIntMapping() {
            this.objectIntMapping = new SingleThreadObjectIntMapping<>();
        }

        @Override
        @Watched
        public synchronized int object2Code(Object object) {
            return this.objectIntMapping.object2Code(object);
        }

        @Override
        @Watched
        public synchronized V code2Object(int code) {
            return this.objectIntMapping.code2Object(code);
        }

        @Override
        public synchronized void clear() {
            this.objectIntMapping.clear();
        }

        @Override
        public synchronized String toString() {
            return this.objectIntMapping.toString();
        }
    }
}
