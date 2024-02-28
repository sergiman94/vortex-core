package com.vortex.gremlin.api.controllers;

import com.vortex.gremlin.api.services.GremlinQueryService;
import com.vortex.gremlin.api.services.GremlinQueryServiceImpl;
import com.vortex.gremlin.models.BodyRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;

import java.util.List;

@Controller
public class VortexGremlinController implements VortexGremlinInterface {

    private static final Logger log = LoggerFactory.getLogger(VortexGremlinController.class);
    private final GremlinQueryService gremlinQueryService = new GremlinQueryServiceImpl();

    @Override
    public ResponseEntity<String> gremlinQuery(BodyRequest body) {
        gremlinQueryService.executeGremlinQuery(body.getQueryCode());
        return new ResponseEntity<String>("{}", HttpStatus.OK);
    }
}


