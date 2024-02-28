
package com.vortex.vortexdb.type;

import com.vortex.vortexdb.backend.id.Id;

import java.util.Set;

public interface Propfiable {

    public Set<Id> properties();
}
