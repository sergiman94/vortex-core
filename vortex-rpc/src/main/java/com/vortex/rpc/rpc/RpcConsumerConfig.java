
package com.vortex.rpc.rpc;

import com.alipay.sofa.rpc.bootstrap.Bootstraps;
import com.alipay.sofa.rpc.bootstrap.ConsumerBootstrap;
import com.alipay.sofa.rpc.client.AbstractCluster;
import com.alipay.sofa.rpc.client.Cluster;
import com.alipay.sofa.rpc.client.ProviderInfo;
import com.alipay.sofa.rpc.config.ConsumerConfig;
import com.alipay.sofa.rpc.core.exception.RpcErrorType;
import com.alipay.sofa.rpc.core.exception.SofaRpcException;
import com.alipay.sofa.rpc.core.request.SofaRequest;
import com.alipay.sofa.rpc.core.response.SofaResponse;
import com.alipay.sofa.rpc.ext.Extension;
import com.alipay.sofa.rpc.ext.ExtensionLoaderFactory;
import com.vortex.common.config.VortexConfig;
import com.vortex.rpc.config.RpcOptions;
import com.vortex.common.util.Log;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RpcConsumerConfig implements RpcServiceConfig4Client {

    private final VortexConfig conf;
    private final String remoteUrls;
    private final Map<String, ConsumerConfig<?>> configs;
    private final List<ConsumerBootstrap<?>> bootstraps;

    static {
         ExtensionLoaderFactory.getExtensionLoader(Cluster.class)
                               .loadExtension(FanoutCluster.class);
    }

    public RpcConsumerConfig(VortexConfig config, String remoteUrls) {
        RpcCommonConfig.initRpcConfigs(config);
        this.conf = config;
        this.remoteUrls = remoteUrls;
        this.configs = Maps.newHashMap();
        this.bootstraps = Lists.newArrayList();
    }

    @Override
    public <T> T serviceProxy(String interfaceId) {
        return this.serviceProxy(null, interfaceId);
    }

    @Override
    public <T> T serviceProxy(String graph, String interfaceId) {
        ConsumerConfig<T> config = this.consumerConfig(graph, interfaceId);
        ConsumerBootstrap<T> bootstrap = Bootstraps.from(config);
        this.bootstraps.add(bootstrap);
        return bootstrap.refer();
    }

    @Override
    public void removeAllServiceProxy() {
        for (ConsumerBootstrap<?> bootstrap : this.bootstraps) {
            bootstrap.unRefer();
        }
    }

    public void destroy() {
        Set<Cluster> clusters = Sets.newHashSet();
        for (ConsumerBootstrap<?> bootstrap : this.bootstraps) {
            bootstrap.unRefer();
            clusters.add(bootstrap.getCluster());
        }
        for (Cluster cluster : clusters) {
            cluster.destroy();
        }
    }

    private <T> ConsumerConfig<T> consumerConfig(String graph,
                                                 String interfaceId) {
        String serviceId;
        if (graph != null) {
            serviceId = interfaceId + ":" + graph;
        } else {
            serviceId = interfaceId;
        }

        @SuppressWarnings("unchecked")
        ConsumerConfig<T> consumerConfig = (ConsumerConfig<T>)
                                           this.configs.get(serviceId);
        if (consumerConfig != null) {
            return consumerConfig;
        }

        assert consumerConfig == null;
        consumerConfig = new ConsumerConfig<>();

        VortexConfig conf = this.conf;
        String protocol = conf.get(RpcOptions.RPC_PROTOCOL);
        int timeout = conf.get(RpcOptions.RPC_CLIENT_READ_TIMEOUT) * 1000;
        int connectTimeout = conf.get(RpcOptions
                                      .RPC_CLIENT_CONNECT_TIMEOUT) * 1000;
        int reconnectPeriod = conf.get(RpcOptions
                                       .RPC_CLIENT_RECONNECT_PERIOD) * 1000;
        int retries = conf.get(RpcOptions.RPC_CLIENT_RETRIES);
        String loadBalancer = conf.get(RpcOptions.RPC_CLIENT_LOAD_BALANCER);

        if (graph != null) {
            consumerConfig.setId(serviceId).setUniqueId(graph);
            // Default is FailoverCluster, set to FanoutCluster to broadcast
            consumerConfig.setCluster("fanout");
        }
        consumerConfig.setInterfaceId(interfaceId)
                      .setProtocol(protocol)
                      .setDirectUrl(this.remoteUrls)
                      .setTimeout(timeout)
                      .setConnectTimeout(connectTimeout)
                      .setReconnectPeriod(reconnectPeriod)
                      .setRetries(retries)
                      .setLoadBalancer(loadBalancer);

        this.configs.put(serviceId, consumerConfig);
        return consumerConfig;
    }

    @Extension("fanout")
    private static class FanoutCluster extends AbstractCluster {

        private static final Logger LOG = Log.logger(FanoutCluster.class);

        public FanoutCluster(ConsumerBootstrap<?> consumerBootstrap) {
            super(consumerBootstrap);
        }

        @Override
        protected SofaResponse doInvoke(SofaRequest request)
                                        throws SofaRpcException {
            List<ProviderInfo> providers = this.getRouterChain()
                                               .route(request, null);
            List<SofaResponse> responses = new ArrayList<>(providers.size());
            List<SofaRpcException> excepts = new ArrayList<>(providers.size());

            for (ProviderInfo provider : providers) {
                try {
                    SofaResponse response = this.doInvoke(request, provider);
                    responses.add(response);
                } catch (SofaRpcException e) {
                    excepts.add(e);
                    LOG.warn("{}.(error {})", e.getMessage(), e.getErrorType());
                }
            }

            if (responses.size() > 0) {
                /*
                 * Just choose the first one as result to return, ignore others
                 * TODO: maybe more strategies should be provided
                 */
                return responses.get(0);
            } else if (excepts.size() > 0) {
                throw excepts.get(0);
            } else {
                assert providers.isEmpty();
                String method = methodName(request);
                throw new SofaRpcException(RpcErrorType.CLIENT_ROUTER,
                                           "No service provider for " + method);
            }
        }

        private SofaResponse doInvoke(SofaRequest request,
                                      ProviderInfo providerInfo) {
            try {
                SofaResponse response = this.filterChain(providerInfo, request);
                if (response != null) {
                    return response;
                }
                String method = methodName(request);
                throw new SofaRpcException(RpcErrorType.CLIENT_UNDECLARED_ERROR,
                          "Failed to call " + method + " on remote server " +
                          providerInfo + ", return null response");
            } catch (Exception e) {
                int error = RpcErrorType.CLIENT_UNDECLARED_ERROR;
                if (e instanceof SofaRpcException) {
                    error = ((SofaRpcException) e).getErrorType();
                }
                String method = methodName(request);
                throw new SofaRpcException(error,
                          "Failed to call " + method + " on remote server " +
                          providerInfo + ", caused by exception: " + e);
            }
        }

        private static String methodName(SofaRequest request) {
            return request.getInterfaceName() + "." +
                   request.getMethodName() + "()";
        }
    }
}
