package com.vortex.client.example;

import com.vortex.client.driver.GraphManager;
import com.vortex.client.driver.VortexClient;
import com.vortex.common.util.DateUtil;

import java.util.Date;
import java.util.List;

public class Playground {

    public static void main(String[] args) {
        // If connect failed will throw a exception.
//        VortexClient vortexClient = VortexClient.builder("http://localhost:8080",
//                "vortex").build();
//
//        GraphManager  graph = vortexClient.graph();
//
//        List vertices = graph.listVertices();
//
//        vertices.forEach(vert -> System.out.println(vert.toString()));

        String string = "Thu Apr 28 20:43:05 COT 2022";
        Date date = DateUtil.now();

        System.out.println(date);


    }
}
