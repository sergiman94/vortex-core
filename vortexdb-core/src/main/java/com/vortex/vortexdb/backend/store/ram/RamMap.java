
package com.vortex.vortexdb.backend.store.ram;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public interface RamMap {

    public void clear();

    public long size();

    public void writeTo(DataOutputStream buffer) throws IOException;

    public void readFrom(DataInputStream buffer) throws IOException;
}
