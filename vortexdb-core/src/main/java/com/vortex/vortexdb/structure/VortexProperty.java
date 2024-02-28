
package com.vortex.vortexdb.structure;

import com.vortex.vortexdb.backend.id.SplicingIdGenerator;
import com.vortex.vortexdb.schema.PropertyKey;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.common.util.E;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.NoSuchElementException;

public abstract class VortexProperty<V> implements Property<V>, GraphType {

    protected final VortexElement owner;
    protected final PropertyKey pkey;
    protected final V value;

    public VortexProperty(VortexElement owner, PropertyKey pkey, V value) {
        E.checkArgument(owner != null, "Property owner can't be null");
        E.checkArgument(pkey != null, "Property key can't be null");
        E.checkArgument(value != null, "Property value can't be null");

        this.owner = owner;
        this.pkey = pkey;
        this.value = pkey.validValueOrThrow(value);
    }

    public PropertyKey propertyKey() {
        return this.pkey;
    }

    public Object id() {
        return SplicingIdGenerator.concat(this.owner.id().asString(),
                                          this.key());
    }

    @Override
    public VortexType type() {
        return VortexType.PROPERTY;
    }

    @Override
    public String name() {
        return this.pkey.name();
    }

    @Override
    public String key() {
        return this.pkey.name();
    }

    @Override
    public V value() throws NoSuchElementException {
        return this.value;
    }

    public Object serialValue(boolean encodeNumber) {
        return this.pkey.serialValue(this.value, encodeNumber);
    }

    @Override
    public boolean isPresent() {
        return null != this.value;
    }

    public boolean isAggregateType() {
        return !this.pkey.aggregateType().isNone();
    }

    @Override
    public VortexElement element() {
        return this.owner;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof VortexProperty)) {
            return false;
        }

        VortexProperty<?> other = (VortexProperty<?>) obj;
        return this.owner.equals(other.owner) && this.pkey.equals(other.pkey);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }
}
