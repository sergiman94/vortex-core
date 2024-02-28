
package com.vortex.api.rpc;

import com.alipay.sofa.rpc.common.utils.StringUtils;
import com.vortex.rpc.rpc.RpcClientProvider;
import com.vortex.rpc.rpc.RpcConsumerConfig;
import com.vortex.vortexdb.auth.AuthManager;
import com.vortex.common.config.VortexConfig;
import com.vortex.api.config.ServerOptions;
import com.vortex.common.util.E;

public class RpcClientProviderWithAuth extends RpcClientProvider {

    private final RpcConsumerConfig authConsumerConfig;

    public RpcClientProviderWithAuth(VortexConfig config) {
        super(config);

        String authUrl = config.get(ServerOptions.AUTH_REMOTE_URL);
        this.authConsumerConfig = StringUtils.isNotBlank(authUrl) ?
                                  new RpcConsumerConfig(config, authUrl) : null;
    }

    public AuthManager authManager() {
        E.checkArgument(this.authConsumerConfig != null,
                        "RpcClient is not enabled, please config option '%s'",
                        ServerOptions.AUTH_REMOTE_URL.name());
        return this.authConsumerConfig.serviceProxy(AuthManager.class);
    }
}
