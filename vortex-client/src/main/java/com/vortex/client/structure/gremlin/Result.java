package com.vortex.client.structure.gremlin;

import com.vortex.client.structure.graph.Edge;
import com.vortex.client.structure.graph.Path;
import com.vortex.client.structure.graph.Vertex;

public class Result {

    private Object object;

    public Result(Object object) {
        this.object = object;
    }

    public Object getObject() {
        return this.object;
    }

    public String getString() {
        return this.object.toString();
    }

    public int getInt() {
        return Integer.parseInt(this.object.toString());
    }

    public byte getByte() {
        return Byte.parseByte(this.object.toString());
    }

    public short getShort() {
        return Short.parseShort(this.object.toString());
    }

    public long getLong() {
        return Long.parseLong(this.object.toString());
    }

    public float getFloat() {
        return Float.parseFloat(this.object.toString());
    }

    public double getDouble() {
        return Double.parseDouble(this.object.toString());
    }

    public boolean getBoolean() {
        return Boolean.parseBoolean(this.object.toString());
    }

    public boolean isNull() {
        return null == this.object;
    }

    public Vertex getVertex() {
        return (Vertex) this.object;
    }

    public Edge getEdge() {
        return (Edge) this.object;
    }

    public Path getPath() {
        return (Path) this.object;
    }
}
