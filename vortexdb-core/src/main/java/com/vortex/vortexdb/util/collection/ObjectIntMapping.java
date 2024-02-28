
package com.vortex.vortexdb.util.collection;

public interface ObjectIntMapping<V> {

    public int object2Code(Object object);

    public V code2Object(int code);

    public void clear();
}
