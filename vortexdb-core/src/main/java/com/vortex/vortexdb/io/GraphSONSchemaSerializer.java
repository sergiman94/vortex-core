
package com.vortex.vortexdb.io;

import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.schema.EdgeLabel;
import com.vortex.vortexdb.schema.IndexLabel;
import com.vortex.vortexdb.schema.PropertyKey;
import com.vortex.vortexdb.schema.VertexLabel;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.vortexdb.type.define.VortexKeys;
import java.util.LinkedHashMap;
import java.util.Map;

public class GraphSONSchemaSerializer {

    public Map<VortexKeys, Object> writeVertexLabel(VertexLabel vertexLabel) {
        Vortex graph = vertexLabel.graph();
        assert graph != null;

        Map<VortexKeys, Object> map = new LinkedHashMap<>();
        map.put(VortexKeys.ID, vertexLabel.id().asLong());
        map.put(VortexKeys.NAME, vertexLabel.name());
        map.put(VortexKeys.ID_STRATEGY, vertexLabel.idStrategy());
        map.put(VortexKeys.PRIMARY_KEYS,
                graph.mapPkId2Name(vertexLabel.primaryKeys()));
        map.put(VortexKeys.NULLABLE_KEYS,
                graph.mapPkId2Name(vertexLabel.nullableKeys()));
        map.put(VortexKeys.INDEX_LABELS,
                graph.mapIlId2Name(vertexLabel.indexLabels()));
        map.put(VortexKeys.PROPERTIES,
                graph.mapPkId2Name(vertexLabel.properties()));
        map.put(VortexKeys.STATUS, vertexLabel.status());
        map.put(VortexKeys.TTL, vertexLabel.ttl());
        String ttlStartTimeName = vertexLabel.ttlStartTimeName();
        if (ttlStartTimeName != null) {
            map.put(VortexKeys.TTL_START_TIME, ttlStartTimeName);
        }
        map.put(VortexKeys.ENABLE_LABEL_INDEX, vertexLabel.enableLabelIndex());
        map.put(VortexKeys.USER_DATA, vertexLabel.userdata());
        return map;
    }

    public Map<VortexKeys, Object> writeEdgeLabel(EdgeLabel edgeLabel) {
        Vortex graph = edgeLabel.graph();
        assert graph != null;

        Map<VortexKeys, Object> map = new LinkedHashMap<>();
        map.put(VortexKeys.ID, edgeLabel.id().asLong());
        map.put(VortexKeys.NAME, edgeLabel.name());
        map.put(VortexKeys.SOURCE_LABEL, edgeLabel.sourceLabelName());
        map.put(VortexKeys.TARGET_LABEL, edgeLabel.targetLabelName());
        map.put(VortexKeys.FREQUENCY, edgeLabel.frequency());
        map.put(VortexKeys.SORT_KEYS,
                graph.mapPkId2Name(edgeLabel.sortKeys()));
        map.put(VortexKeys.NULLABLE_KEYS,
                graph.mapPkId2Name(edgeLabel.nullableKeys()));
        map.put(VortexKeys.INDEX_LABELS,
                graph.mapIlId2Name(edgeLabel.indexLabels()));
        map.put(VortexKeys.PROPERTIES,
                graph.mapPkId2Name(edgeLabel.properties()));
        map.put(VortexKeys.STATUS, edgeLabel.status());
        map.put(VortexKeys.TTL, edgeLabel.ttl());
        String ttlStartTimeName = edgeLabel.ttlStartTimeName();
        if (ttlStartTimeName != null) {
            map.put(VortexKeys.TTL_START_TIME, ttlStartTimeName);
        }
        map.put(VortexKeys.ENABLE_LABEL_INDEX, edgeLabel.enableLabelIndex());
        map.put(VortexKeys.USER_DATA, edgeLabel.userdata());
        return map;
    }

    public Map<VortexKeys, Object> writePropertyKey(PropertyKey propertyKey) {
        Vortex graph = propertyKey.graph();
        assert graph != null;

        Map<VortexKeys, Object> map = new LinkedHashMap<>();
        map.put(VortexKeys.ID, propertyKey.id().asLong());
        map.put(VortexKeys.NAME, propertyKey.name());
        map.put(VortexKeys.DATA_TYPE, propertyKey.dataType());
        map.put(VortexKeys.CARDINALITY, propertyKey.cardinality());
        map.put(VortexKeys.AGGREGATE_TYPE, propertyKey.aggregateType());
        map.put(VortexKeys.WRITE_TYPE, propertyKey.writeType());
        map.put(VortexKeys.PROPERTIES,
                graph.mapPkId2Name(propertyKey.properties()));
        map.put(VortexKeys.STATUS, propertyKey.status());
        map.put(VortexKeys.USER_DATA, propertyKey.userdata());
        return map;
    }

    public Map<VortexKeys, Object> writeIndexLabel(IndexLabel indexLabel) {
        Vortex graph = indexLabel.graph();
        assert graph != null;

        Map<VortexKeys, Object> map = new LinkedHashMap<>();
        map.put(VortexKeys.ID, indexLabel.id().asLong());
        map.put(VortexKeys.NAME, indexLabel.name());
        map.put(VortexKeys.BASE_TYPE, indexLabel.baseType());
        if (indexLabel.baseType() == VortexType.VERTEX_LABEL) {
            map.put(VortexKeys.BASE_VALUE,
                    graph.vertexLabel(indexLabel.baseValue()).name());
        } else {
            assert indexLabel.baseType() == VortexType.EDGE_LABEL;
            map.put(VortexKeys.BASE_VALUE,
                    graph.edgeLabel(indexLabel.baseValue()).name());
        }
        map.put(VortexKeys.INDEX_TYPE, indexLabel.indexType());
        map.put(VortexKeys.FIELDS, graph.mapPkId2Name(indexLabel.indexFields()));
        map.put(VortexKeys.STATUS, indexLabel.status());
        map.put(VortexKeys.USER_DATA, indexLabel.userdata());
        return map;
    }
}
