
package com.vortex.vortexdb.plugin;

import com.vortex.vortexdb.VortexException;
import com.vortex.vortexdb.VortexFactory;
import com.vortex.common.util.ReflectionUtil;
import com.google.common.reflect.ClassPath;
import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.DefaultImportCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.ImportCustomizer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class VortexGremlinPlugin extends AbstractGremlinPlugin {

    private static final String PACKAGE = "com.vortex.vortexdb.type.define";
    private static final String NAME = "Vortex";

    private static final VortexGremlinPlugin instance;
    private static final ImportCustomizer imports;

    static {
        instance = new VortexGremlinPlugin();

        Iterator<ClassPath.ClassInfo> classInfos;
        try {
            classInfos = ReflectionUtil.classes(PACKAGE);
        } catch (IOException e) {
            throw new VortexException("Failed to scan classes under package %s",
                                    e, PACKAGE);
        }

        @SuppressWarnings("rawtypes")
        Set<Class> classes = new HashSet<>();
        classInfos.forEachRemaining(classInfo -> classes.add(classInfo.load()));
        // Add entrance class: graph = VortexFactory.open("vortex.properties")
        classes.add(VortexFactory.class);

        imports = DefaultImportCustomizer.build()
                                         .addClassImports(classes)
                                         .create();
    }

    public VortexGremlinPlugin() {
        super(NAME, imports);
    }

    public static VortexGremlinPlugin instance() {
        return instance;
    }
}
