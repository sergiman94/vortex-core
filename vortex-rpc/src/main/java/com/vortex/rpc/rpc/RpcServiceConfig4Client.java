
package com.vortex.rpc.rpc;

public interface RpcServiceConfig4Client {

    public <T> T serviceProxy(String interfaceId);

    public <T> T serviceProxy(String graph, String interfaceId);

    public default <T> T serviceProxy(Class<T> clazz) {
        return this.serviceProxy(clazz.getName());
    }

    public default <T> T serviceProxy(String graph, Class<T> clazz) {
        return this.serviceProxy(graph, clazz.getName());
    }

    public void removeAllServiceProxy();
}
