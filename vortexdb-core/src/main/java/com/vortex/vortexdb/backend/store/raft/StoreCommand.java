
package com.vortex.vortexdb.backend.store.raft;

import com.vortex.vortexdb.backend.serializer.BytesBuffer;
import com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreAction;
import com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreType;

public final class StoreCommand {

    public static final int HEADER_SIZE = 2;

    private final StoreType type;
    private final StoreAction action;
    private final byte[] data;
    private final boolean forwarded;

    public StoreCommand(StoreType type, StoreAction action, byte[] data) {
        this(type, action, data, false);
    }

    public StoreCommand(StoreType type, StoreAction action,
                        byte[] data, boolean forwarded) {
        this.type = type;
        this.action = action;
        if (data == null) {
            this.data = new byte[HEADER_SIZE];
        } else {
            assert data.length >= HEADER_SIZE;
            this.data = data;
        }
        this.data[0] = (byte) this.type.getNumber();
        this.data[1] = (byte) this.action.getNumber();
        this.forwarded = forwarded;
    }

    public StoreType type() {
        return this.type;
    }

    public StoreAction action() {
        return this.action;
    }

    public byte[] data() {
        return this.data;
    }

    public boolean forwarded() {
        return this.forwarded;
    }

    public static void writeHeader(BytesBuffer buffer) {
        buffer.write((byte) 0);
        buffer.write((byte) 0);
    }

    public static byte[] wrap(byte value) {
        byte[] bytes = new byte[HEADER_SIZE + 1];
        bytes[2] = value;
        return bytes;
    }

    public static StoreCommand fromBytes(byte[] bytes) {
        StoreType type = StoreType.valueOf(bytes[0]);
        StoreAction action = StoreAction.valueOf(bytes[1]);
        return new StoreCommand(type, action, bytes);
    }

    @Override
    public String toString() {
        return String.format("StoreCommand{type=%s,action=%s}",
                             this.type.name(), this.action.name());
    }
}
