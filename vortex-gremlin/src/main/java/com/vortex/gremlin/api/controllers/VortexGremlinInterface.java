package com.vortex.gremlin.api.controllers;

import com.vortex.gremlin.models.BodyRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import java.util.List;

public interface VortexGremlinInterface {

    @RequestMapping(value = "/gremlin-query",
            produces = {"application/json", "application/xml"},
            consumes = {"application/json", "application/xml"},
            method = RequestMethod.POST)
    ResponseEntity<String> gremlinQuery(@RequestBody BodyRequest body);
}
