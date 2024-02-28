package com.vortex.client.api.schema;

import com.vortex.client.api.API;
import com.vortex.client.client.RestClient;
import com.vortex.client.exception.NotSupportException;
import com.vortex.common.rest.RestResult;
import com.vortex.client.structure.SchemaElement;

import java.util.List;
import java.util.Map;

public class SchemaAPI extends API {

    private static final String PATH = "graphs/%s/%s";

    public SchemaAPI(RestClient client, String graph) {
        super(client);
        this.path(PATH, graph, this.type());
    }

    @SuppressWarnings("unchecked")
    public Map<String, List<SchemaElement>> list() {
        if (this.client.apiVersionLt("0.66")) {
            throw new NotSupportException("schema get api");
        }
        RestResult result = this.client.get(this.path());
        return result.readObject(Map.class);
    }

    @Override
    protected String type() {
        return "schema";
    }
}
