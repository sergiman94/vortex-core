
package com.vortex.vortexdb.structure;

import com.vortex.vortexdb.schema.EdgeLabel;
import com.vortex.vortexdb.schema.PropertyKey;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.common.util.E;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

public class VortexEdgeProperty<V> extends VortexProperty<V> {

    public VortexEdgeProperty(VortexElement owner, PropertyKey key, V value) {
        super(owner, key, value);
    }

    @Override
    public VortexType type() {
        return this.pkey.aggregateType().isNone() ?
               VortexType.PROPERTY : VortexType.AGGR_PROPERTY_E;
    }

    @Override
    public VortexEdge element() {
        assert this.owner instanceof VortexEdge;
        return (VortexEdge) this.owner;
    }

    @Override
    public void remove() {
        assert this.owner instanceof VortexEdge;
        EdgeLabel edgeLabel = ((VortexEdge) this.owner).schemaLabel();
        E.checkArgument(edgeLabel.nullableKeys().contains(
                        this.propertyKey().id()),
                        "Can't remove non-null edge property '%s'", this);
        this.owner.graph().removeEdgeProperty(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Property)) {
            return false;
        }
        return ElementHelper.areEqual(this, obj);
    }

    public VortexEdgeProperty<V> switchEdgeOwner() {
        assert this.owner instanceof VortexEdge;
        return new VortexEdgeProperty<V>(((VortexEdge) this.owner).switchOwner(),
                                       this.pkey, this.value);
    }
}
