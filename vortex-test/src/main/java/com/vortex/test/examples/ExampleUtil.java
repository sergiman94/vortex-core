/*
 * Copyright 2017 Vortex Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.vortex.test.examples;

import com.vortex.vortexdb.VortexException;
import com.vortex.vortexdb.VortexFactory;
import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.backend.id.IdGenerator;
import com.vortex.distr.RegisterUtil;
import com.vortex.common.perf.PerfUtil;
import com.vortex.vortexdb.task.VortexTask;
import com.vortex.vortexdb.task.TaskScheduler;
import com.vortex.vortexdb.type.define.NodeRole;

import java.io.File;
import java.util.Iterator;
import java.util.concurrent.TimeoutException;

public class ExampleUtil {

    private static boolean registered = false;

    public static void registerPlugins() {
        if (registered) {
            return;
        }
        registered = true;

//        RegisterUtil.registerCassandra();
//        RegisterUtil.registerScyllaDB();
//        RegisterUtil.registerHBase();
//        RegisterUtil.registerRocksDB();
//        RegisterUtil.registerMysql();
//        RegisterUtil.registerPalo();
    }

    public static Vortex loadGraph() {
        return loadGraph(true, false);
    }

    public static Vortex loadGraph(boolean needClear, boolean needProfile) {
        if (needProfile) {
            profile();
        }

        //registerPlugins();

        String conf = "vortex.properties";
        try {
            String path = ExampleUtil.class.getClassLoader()
                                     .getResource(conf).getPath();
            File file = new File(path);
            if (file.exists() && file.isFile()) {
                conf = path;
            }
        } catch (Exception ignored) {
        }

        Vortex graph = VortexFactory.open(conf);

        if (needClear) {
            graph.clearBackend();
        }
        graph.initBackend();
        graph.serverStarted(IdGenerator.of("server1"), NodeRole.MASTER);

        return graph;
    }

    public static void profile() {
        try {
            PerfUtil.instance().profilePackage("com.vortex.common");
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    public static void waitAllTaskDone(Vortex graph) {
        TaskScheduler scheduler = graph.taskScheduler();
        Iterator<VortexTask<Object>> tasks = scheduler.tasks(null, -1L, null);
        while (tasks.hasNext()) {
            try {
                scheduler.waitUntilTaskCompleted(tasks.next().id(), 20L);
            } catch (TimeoutException e) {
                throw new VortexException("Failed to wait task done", e);
            }
        }
    }
}
