
package com.vortex.rpc.rpc;

import com.alipay.sofa.rpc.common.RpcConfigs;
import com.vortex.common.config.VortexConfig;
import com.vortex.rpc.config.RpcOptions;

import java.util.Map;

public class RpcCommonConfig {

    public static void initRpcConfigs(VortexConfig config) {
        RpcConfigs.putValue("rpc.config.order",
                            config.get(RpcOptions.RPC_CONFIG_ORDER));
        RpcConfigs.putValue("logger.impl",
                            config.get(RpcOptions.RPC_LOGGER_IMPL));
    }

    public static void initRpcConfigs(String key, Object value) {
        RpcConfigs.putValue(key, value);
    }

    public static void initRpcConfigs(Map<String, Object> conf) {
        for (Map.Entry<String, Object> entry : conf.entrySet()) {
            RpcConfigs.putValue(entry.getKey(), entry.getValue());
        }
    }
}
