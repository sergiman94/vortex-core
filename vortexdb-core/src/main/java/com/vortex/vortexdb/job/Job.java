
package com.vortex.vortexdb.job;

public interface Job<V> {

    public String type();

    public V execute() throws Exception;
}
