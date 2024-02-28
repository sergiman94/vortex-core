package com.vortex.gremlin.api.services;

import com.vortex.client.driver.GremlinManager;
import com.vortex.client.driver.VortexClient;
import com.vortex.client.structure.gremlin.ResultSet;
import org.springframework.stereotype.Service;

@Service
public class GremlinQueryServiceImpl implements GremlinQueryService {
    @Override
    public void executeGremlinQuery(String queryCode) {

       try {
           VortexClient vortexClient = VortexClient.builder("http://localhost:8080", "vortex").build();

           GremlinManager gremlinManager = vortexClient.gremlin();

           ResultSet resultSet = gremlinManager.gremlin(queryCode).execute();

           System.out.println(resultSet.toString());
       } catch(Exception e) {
           System.out.println(e.getLocalizedMessage());
       }
    }
}
