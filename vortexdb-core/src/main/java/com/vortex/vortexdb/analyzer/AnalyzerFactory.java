
package com.vortex.vortexdb.analyzer;

import com.vortex.vortexdb.VortexException;
import com.vortex.vortexdb.backend.serializer.SerializerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AnalyzerFactory {

    private static Map<String, Class<? extends Analyzer>> analyzers;

    static {
        analyzers = new ConcurrentHashMap<>();
    }

    public static Analyzer analyzer(String name, String mode) {
        name = name.toLowerCase();
        switch (name) {
            case "word":
                return new WordAnalyzer(mode);
            case "ansj":
                return new AnsjAnalyzer(mode);
            case "hanlp":
                return new HanLPAnalyzer(mode);
            case "smartcn":
                return new SmartCNAnalyzer(mode);
            case "jieba":
                return new JiebaAnalyzer(mode);
            case "jcseg":
                return new JcsegAnalyzer(mode);
            case "mmseg4j":
                return new MMSeg4JAnalyzer(mode);
            case "ikanalyzer":
                return new IKAnalyzer(mode);
            default:
                return customizedAnalyzer(name, mode);
        }
    }

    private static Analyzer customizedAnalyzer(String name, String mode) {
        Class<? extends Analyzer> clazz = analyzers.get(name);
        if (clazz == null) {
            throw new VortexException("Not exists analyzer: %s", name);
        }

        assert Analyzer.class.isAssignableFrom(clazz);
        try {
            return clazz.getConstructor(String.class).newInstance(mode);
        } catch (Exception e) {
            throw new VortexException(
                      "Failed to construct analyzer '%s' with mode '%s'",
                      e, name, mode);
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static void register(String name, String classPath) {
        ClassLoader classLoader = SerializerFactory.class.getClassLoader();
        Class<?> clazz;
        try {
            clazz = classLoader.loadClass(classPath);
        } catch (Exception e) {
            throw new VortexException("Load class path '%s' failed",
                                    e, classPath);
        }

        // Check subclass
        if (!Analyzer.class.isAssignableFrom(clazz)) {
            throw new VortexException("Class '%s' is not a subclass of " +
                                    "class Analyzer", classPath);
        }

        // Check exists
        if (analyzers.containsKey(name)) {
            throw new VortexException("Exists analyzer: %s(%s)",
                                    name, analyzers.get(name).getName());
        }

        // Register class
        analyzers.put(name, (Class) clazz);
    }
}
