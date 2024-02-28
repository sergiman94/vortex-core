package com.vortex.client.api.schema;

import com.vortex.client.api.API;
import com.vortex.client.client.RestClient;
import com.vortex.client.structure.SchemaElement;

public abstract class SchemaElementAPI extends API {

    private static final String PATH = "graphs/%s/schema/%s";

    public SchemaElementAPI(RestClient client, String graph) {
        super(client);
        this.path(PATH, graph, this.type());
    }

    protected abstract Object checkCreateOrUpdate(SchemaElement schemaElement);
}
