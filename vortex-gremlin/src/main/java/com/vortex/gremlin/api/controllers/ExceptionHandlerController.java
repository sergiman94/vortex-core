package com.vortex.gremlin.api.controllers;

import com.vortex.gremlin.api.exceptions.ApiException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import java.util.HashMap;

@ControllerAdvice
public class ExceptionHandlerController {
        @ExceptionHandler(value = ApiException.class)
        public ResponseEntity<Object> exception(ApiException exception) {

            HashMap<String,String> map=new HashMap<>();
            map.put("Message",exception.getMessage());
            map.put("Status", String.valueOf(exception.getStatusCode()));

            return new ResponseEntity<>(map,HttpStatus.resolve(exception.getStatusCode()));
        }
    }

