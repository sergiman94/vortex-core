
package com.vortex.vortexdb.structure;

import com.vortex.vortexdb.exception.NotSupportException;
import com.vortex.vortexdb.schema.PropertyKey;
import com.vortex.vortexdb.schema.VertexLabel;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.common.util.E;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.Iterator;

public class VortexVertexProperty<V> extends VortexProperty<V>
                                   implements VertexProperty<V> {

    public VortexVertexProperty(VortexElement owner, PropertyKey key, V value) {
        super(owner, key, value);
    }

    @Override
    public VortexType type() {
        return this.pkey.aggregateType().isNone() ?
               VortexType.PROPERTY : VortexType.AGGR_PROPERTY_V;
    }

    @Override
    public <U> Property<U> property(String key, U value) {
        throw new NotSupportException("nested property");
    }

    @Override
    public VortexVertex element() {
        assert this.owner instanceof VortexVertex;
        return (VortexVertex) this.owner;
    }

    @Override
    public void remove() {
        assert this.owner instanceof VortexVertex;
        VertexLabel vertexLabel = ((VortexVertex) this.owner).schemaLabel();
        E.checkArgument(vertexLabel.nullableKeys().contains(
                        this.propertyKey().id()),
                        "Can't remove non-null vertex property '%s'", this);
        this.owner.graph().removeVertexProperty(this);
    }

    @Override
    public <U> Iterator<Property<U>> properties(String... propertyKeys) {
        throw new NotSupportException("nested property");
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof VertexProperty)) {
            return false;
        }
        @SuppressWarnings("unchecked")
        VertexProperty<V> other = (VertexProperty<V>) obj;
        return this.id().equals(other.id());
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode((Element) this);
    }
}
