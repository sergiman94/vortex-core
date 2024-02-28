
package com.vortex.vortexdb.plugin;

import com.vortex.vortexdb.analyzer.AnalyzerFactory;
import com.vortex.vortexdb.backend.serializer.SerializerFactory;
import com.vortex.vortexdb.backend.store.BackendProviderFactory;
import com.vortex.common.config.OptionSpace;

public interface VortexPlugin {

    public String name();

    public void register();

    public String supportsMinVersion();

    public String supportsMaxVersion();

    public static void registerOptions(String name, String classPath) {
        OptionSpace.register(name, classPath);
    }

    public static void registerBackend(String name, String classPath) {
        BackendProviderFactory.register(name, classPath);
    }

    public static void registerSerializer(String name, String classPath) {
        SerializerFactory.register(name, classPath);
    }

    public static void registerAnalyzer(String name, String classPath) {
        AnalyzerFactory.register(name, classPath);
    }
}
