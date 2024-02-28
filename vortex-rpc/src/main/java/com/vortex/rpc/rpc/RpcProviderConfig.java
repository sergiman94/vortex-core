
package com.vortex.rpc.rpc;

import com.alipay.sofa.rpc.config.ProviderConfig;
import com.vortex.common.util.E;
import com.google.common.collect.Maps;

import java.util.Map;

public class RpcProviderConfig implements RpcServiceConfig4Server {

    private final Map<String, ProviderConfig<?>> configs = Maps.newHashMap();

    @Override
    public <T, S extends T> String addService(Class<T> clazz, S serviceImpl) {
        return this.addService(null, clazz.getName(), serviceImpl);
    }

    @Override
    public <T, S extends T> String addService(String graph,
                                              Class<T> clazz,
                                              S serviceImpl) {
        return this.addService(graph, clazz.getName(), serviceImpl);
    }

    private <T, S extends T> String addService(String graph,
                                               String interfaceId,
                                               S serviceImpl) {
        ProviderConfig<T> providerConfig = new ProviderConfig<>();
        String serviceId;
        if (graph != null) {
            serviceId = interfaceId + ":" + graph;
            providerConfig.setId(serviceId).setUniqueId(graph);
        } else {
            serviceId = interfaceId;
        }

        providerConfig.setInterfaceId(interfaceId)
                      .setRef(serviceImpl);

        E.checkArgument(!this.configs.containsKey(serviceId),
                        "Not allowed to add service already exist: '%s'",
                        serviceId);
        this.configs.put(serviceId, providerConfig);
        return serviceId;
    }

    @Override
    public void removeService(String serviceId) {
        ProviderConfig<?> config = this.configs.remove(serviceId);
        E.checkArgument(config != null,
                        "The service '%s' doesn't exist", serviceId);
        config.unExport();
    }

    @Override
    public void removeAllService() {
        for (ProviderConfig<?> config : this.configs.values()) {
            config.unExport();
        }
        this.configs.clear();
    }

    public Map<String, ProviderConfig<?>> configs() {
        return this.configs;
    }
}
