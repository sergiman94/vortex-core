
package com.vortex.rpc.rpc;

import com.alipay.sofa.rpc.common.utils.StringUtils;
import com.vortex.common.config.VortexConfig;
import com.vortex.rpc.config.RpcOptions;
import com.vortex.common.util.E;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

public class RpcClientProvider {

    private final RpcConsumerConfig consumerConfig;

    public RpcClientProvider(VortexConfig config) {
        // TODO: fetch from registry server
        String rpcUrl = config.get(RpcOptions.RPC_REMOTE_URL);
        String selfUrl = config.get(RpcOptions.RPC_SERVER_HOST) + ":" +
                         config.get(RpcOptions.RPC_SERVER_PORT);
        rpcUrl = excludeSelfUrl(rpcUrl, selfUrl);
        this.consumerConfig = StringUtils.isNotBlank(rpcUrl) ?
                              new RpcConsumerConfig(config, rpcUrl) : null;
    }
    public boolean enabled() {
        return this.consumerConfig != null;
    }

    public RpcConsumerConfig config() {
        E.checkArgument(this.consumerConfig != null,
                        "RpcClient is not enabled, please config option '%s' " +
                        "and ensure to add an address other than self service",
                        RpcOptions.RPC_REMOTE_URL.name());
        return this.consumerConfig;
    }

    public void unreferAll() {
        if (this.consumerConfig != null) {
            this.consumerConfig.removeAllServiceProxy();
        }
    }

    public void destroy() {
        if (this.consumerConfig != null) {
            this.consumerConfig.destroy();
        }
    }

    protected static String excludeSelfUrl(String rpcUrl, String selfUrl) {
        String[] urls = StringUtils.splitWithCommaOrSemicolon(rpcUrl);
        // Keep urls order via LinkedHashSet
        Set<String> urlSet = new LinkedHashSet<>(Arrays.asList(urls));
        urlSet.remove(selfUrl);
        return String.join(",", urlSet);
    }
}
