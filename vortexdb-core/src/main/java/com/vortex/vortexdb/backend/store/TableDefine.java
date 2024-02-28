
package com.vortex.vortexdb.backend.store;

import com.vortex.vortexdb.type.define.VortexKeys;
import com.vortex.common.util.InsertionOrderUtil;
import com.google.common.collect.ImmutableMap;

import java.util.*;

public class TableDefine {

    private final Map<VortexKeys, String> columns;
    private final List<VortexKeys> keys;
    private final Map<String, String> typesMapping;

    public TableDefine() {
        this.columns = InsertionOrderUtil.newMap();
        this.keys = InsertionOrderUtil.newList();
        this.typesMapping = ImmutableMap.of();
    }

    public TableDefine(Map<String, String> typesMapping) {
        this.columns = InsertionOrderUtil.newMap();
        this.keys = InsertionOrderUtil.newList();
        this.typesMapping = typesMapping;
    }

    public TableDefine column(VortexKeys key, String... desc) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < desc.length; i++) {
            String type = desc[i];
            // The first element of 'desc' is column data type, which may be
            // mapped to actual data type supported by backend store
            if (i == 0 && this.typesMapping.containsKey(type)) {
                type = this.typesMapping.get(type);
            }
            assert type != null;
            sb.append(type);
            if (i != desc.length - 1) {
                sb.append(" ");
            }
        }
        this.columns.put(key, sb.toString());
        return this;
    }

    public Map<VortexKeys, String> columns() {
        return Collections.unmodifiableMap(this.columns);
    }

    public Set<VortexKeys> columnNames() {
        return this.columns.keySet();
    }

    public Collection<String> columnTypes() {
        return this.columns.values();
    }

    public void keys(VortexKeys... keys) {
        this.keys.addAll(Arrays.asList(keys));
    }

    public List<VortexKeys> keys() {
        return Collections.unmodifiableList(this.keys);
    }
}
