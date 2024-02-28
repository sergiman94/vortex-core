

package com.vortex.vortexdb.io;

import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.Io;

public class VortexIoRegistry extends AbstractIoRegistry {

    private static final VortexIoRegistry instance =
                         new VortexIoRegistry();

    public static VortexIoRegistry instance() {
        return instance;
    }

    private VortexIoRegistry() {
        VortexGryoModule.register(this);
        VortexSONModule.register(this);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void register(Class<? extends Io> ioClass, Class clazz, Object ser) {
        super.register(ioClass, clazz, ser);
    }
}
