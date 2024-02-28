
package com.vortex.vortexdb.schema;

import com.vortex.vortexdb.exception.NotAllowException;
import com.vortex.vortexdb.type.define.Action;

import java.util.HashMap;
import java.util.Map;

public class Userdata extends HashMap<String, Object> {

    private static final long serialVersionUID = -1235451175617197049L;

    public static final String CREATE_TIME = "~create_time";
    public static final String DEFAULT_VALUE = "~default_value";

    public Userdata() {
    }

    public Userdata(Map<String, Object> map) {
        this.putAll(map);
    }

    public static void check(Userdata userdata, Action action) {
        if (userdata == null) {
            return;
        }
        switch (action) {
            case INSERT:
            case APPEND:
                for (Entry<String, Object> e : userdata.entrySet()) {
                    if (e.getValue() == null) {
                        throw new NotAllowException(
                                  "Not allowed to pass null userdata value " +
                                  "when create or append schema");
                    }
                }
                break;
            case ELIMINATE:
            case DELETE:
                // pass
                break;
            default:
                throw new AssertionError(String.format(
                          "Unknown schema action '%s'", action));
        }
    }
}
