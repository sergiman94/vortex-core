package com.vortex.client.structure;

import java.util.Objects;

public abstract class Element {

    public abstract String type();

    public abstract Object id();

    @Override
    public int hashCode() {
        return this.id().hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || this.getClass() != other.getClass()) {
            return false;
        }
        return Objects.equals(this.id(), ((Element) other).id());
    }

    @Override
    public String toString() {
        return String.format("%s(type %s)", this.id(), this.type());
    }
}
