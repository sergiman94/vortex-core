
package com.vortex.vortexdb.traversal.optimize;

import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.id.IdGenerator;

public class Text {

    public static ConditionP contains(String value) {
        return ConditionP.textContains(value);
    }

    public static Id uuid(String id) {
        return IdGenerator.of(id, true);
    }
}
