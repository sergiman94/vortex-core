package com.vortex.gremlin.api.services;

import com.vortex.gremlin.models.BodyRequest;

public interface GremlinQueryService {

    void executeGremlinQuery(String queryCode);
}
