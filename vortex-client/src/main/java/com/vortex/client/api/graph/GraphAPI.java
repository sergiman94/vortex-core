package com.vortex.client.api.graph;

import com.vortex.client.api.API;
import com.vortex.client.client.RestClient;
import com.vortex.common.util.E;
import com.vortex.client.util.JsonUtil;
import org.glassfish.jersey.uri.UriComponent;
import org.glassfish.jersey.uri.UriComponent.Type;

import java.util.Map;
import java.util.UUID;

public abstract class GraphAPI extends API {

    private static final String PATH = "graphs/%s/graph/%s";

    private final String batchPath;

    public GraphAPI(RestClient client, String graph) {
        super(client);
        this.path(PATH, graph, this.type());
        this.batchPath = String.join("/", this.path(), "batch");
    }

    public String batchPath() {
        return this.batchPath;
    }

    public static String formatVertexId(Object id) {
        return formatVertexId(id, false);
    }

    public static String formatVertexId(Object id, boolean allowNull) {
        if (!allowNull) {
            E.checkArgumentNotNull(id, "The vertex id can't be null");
        } else {
            if (id == null) {
                return null;
            }
        }
        boolean uuid = id instanceof UUID;
        if (uuid) {
            id = id.toString();
        }
        E.checkArgument(id instanceof String || id instanceof Number,
                        "The vertex id must be either String or " +
                        "Number, but got '%s'", id);
        return (uuid ? "U" : "") + JsonUtil.toJson(id);
    }

    public static String formatProperties(Map<String, Object> properties) {
        if (properties == null) {
            return null;
        }
        String json = JsonUtil.toJson(properties);
        /*
         * Don't use UrlEncoder.encode, it encoded the space as `+`,
         * which will invalidate the jersey's automatic decoding
         * because it considers the space to be encoded as `%2F`
         */
        return encode(json);
    }

    public static String encode(String raw) {
        return UriComponent.encode(raw, Type.QUERY_PARAM_SPACE_ENCODED);
    }
}
