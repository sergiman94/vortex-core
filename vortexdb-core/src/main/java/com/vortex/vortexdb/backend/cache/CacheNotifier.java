
package com.vortex.vortexdb.backend.cache;

import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.type.VortexType;

public interface CacheNotifier extends AutoCloseable {

    public void invalid(VortexType type, Id id);

    public void invalid2(VortexType type, Object[] ids);

    public void clear(VortexType type);

    public void reload();

    public interface GraphCacheNotifier extends CacheNotifier {}

    public interface SchemaCacheNotifier extends CacheNotifier {}
}
