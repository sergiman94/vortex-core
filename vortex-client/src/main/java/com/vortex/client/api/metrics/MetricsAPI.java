package com.vortex.client.api.metrics;

import com.vortex.client.api.API;
import com.vortex.client.client.RestClient;
import com.vortex.common.rest.RestResult;
import com.vortex.client.structure.constant.VortexType;
import com.vortex.client.util.CommonUtil;

import java.util.Map;

public class MetricsAPI extends API {

    public MetricsAPI(RestClient client) {
        super(client);
        this.path(this.type());
    }

    @Override
    protected String type() {
        return VortexType.METRICS.string();
    }

    @SuppressWarnings("unchecked")
    public Map<String, Map<String, Object>> system() {
        RestResult result = this.client.get(this.path(), "system");
        Map<?, ?> map = result.readObject(Map.class);
        CommonUtil.checkMapClass(map, String.class, Map.class);
        for (Object mapValue : map.values()) {
            CommonUtil.checkMapClass(mapValue, String.class, Object.class);
        }
        return (Map<String, Map<String, Object>>) map;
    }

    @SuppressWarnings("unchecked")
    public Map<String, Map<String, Object>> backend() {
        RestResult result = this.client.get(this.path(), "backend");
        Map<?, ?> map = result.readObject(Map.class);
        CommonUtil.checkMapClass(map, String.class, Map.class);
        for (Object mapValue : map.values()) {
            CommonUtil.checkMapClass(mapValue, String.class, Object.class);
        }
        return (Map<String, Map<String, Object>>) map;
    }

    public Map<String, Object> backend(String graph) {
        return this.backend().get(graph);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Map<String, Object>> all() {
        RestResult result = this.client.get(this.path());
        Map<?, ?> map = result.readObject(Map.class);
        CommonUtil.checkMapClass(map, String.class, Map.class);
        for (Object mapValue : map.values()) {
            CommonUtil.checkMapClass(mapValue, String.class, Object.class);
        }
        return (Map<String, Map<String, Object>>) map;
    }
}
