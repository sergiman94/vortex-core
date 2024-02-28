package com.vortex.client.structure.graph;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Shard is used for backend storage (like cassandra, hbase) scanning
 * operations. Each shard represents a range of tokens for a node.
 * Reading data from a given shard does not cross multiple nodes.
 */
public class Shard {

    @JsonProperty("start")
    private String start;
    @JsonProperty("end")
    private String end;
    @JsonProperty("length")
    private long length;

    public Shard() {
    }

    public Shard(String start, String end, long length) {
        this.start = start;
        this.end = end;
        this.length = length;
    }

    public String start() {
        return this.start;
    }

    public void start(String start) {
        this.start = start;
    }

    public String end() {
        return this.end;
    }

    public void end(String end) {
        this.end = end;
    }

    public long length() {
        return this.length;
    }

    public void length(long length) {
        this.length = length;
    }

    @Override
    public String toString() {
        return String.format("Shard{start=%s, end=%s, length=%s}",
                             this.start, this.end, this.length);
    }
}