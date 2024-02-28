
package com.vortex.api.api.traversers;

import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.query.ConditionQuery;
import com.vortex.vortexdb.structure.VortexVertex;
import com.vortex.vortexdb.traversal.optimize.TraversalUtil;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.vortexdb.type.define.VortexKeys;
import com.vortex.common.util.E;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;

public class Vertices {

    @JsonProperty("ids")
    public Set<Object> ids;
    @JsonProperty("label")
    public String label;
    @JsonProperty("properties")
    public Map<String, Object> properties;

    public Iterator<Vertex> vertices(Vortex g) {
        Map<String, Object> props = this.properties;
        E.checkArgument(!((this.ids == null || this.ids.isEmpty()) &&
                        (props == null || props.isEmpty()) &&
                        this.label == null), "No source vertices provided");
        Iterator<Vertex> iterator;
        if (this.ids != null && !this.ids.isEmpty()) {
            List<Id> sourceIds = new ArrayList<>(this.ids.size());
            for (Object id : this.ids) {
                sourceIds.add(VortexVertex.getIdValue(id));
            }
            iterator = g.vertices(sourceIds.toArray());
            E.checkArgument(iterator.hasNext(),
                            "Not exist source vertices with ids %s",
                            this.ids);
        } else {
            ConditionQuery query = new ConditionQuery(VortexType.VERTEX);
            if (this.label != null) {
                Id label = g.vertexLabel(this.label).id();
                query.eq(VortexKeys.LABEL, label);
            }
            if (props != null && !props.isEmpty()) {
                Map<Id, Object> pks = TraversalUtil.transProperties(g, props);
                TraversalUtil.fillConditionQuery(query, pks, g);
            }
            assert !query.empty();
            iterator = g.vertices(query);
            E.checkArgument(iterator.hasNext(), "Not exist source vertex " +
                            "with label '%s' and properties '%s'",
                            this.label, props);
        }
        return iterator;
    }

    @Override
    public String toString() {
        return String.format("SourceVertex{ids=%s,label=%s,properties=%s}",
                             this.ids, this.label, this.properties);
    }
}
