
package com.vortex.vortexdb.exception;

import com.vortex.vortexdb.VortexException;
import com.vortex.vortexdb.type.VortexType;

public class ExistedException extends VortexException {

    private static final long serialVersionUID = 5152465646323494840L;

    public ExistedException(VortexType type, Object arg) {
        this(type.readableName(), arg);
    }

    public ExistedException(String type, Object arg) {
        super("The %s '%s' has existed", type, arg);
    }
}
