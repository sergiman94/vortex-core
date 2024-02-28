
package com.vortex.rpc.rpc;

public interface RpcServiceConfig4Server {

    public <T, S extends T> String addService(Class<T> clazz, S serviceImpl);

    public <T, S extends T> String addService(String graph,
                                              Class<T> clazz, S serviceImpl);

    public void removeService(String serviceId);

    public void removeAllService();
}
