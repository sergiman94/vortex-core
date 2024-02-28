
package com.vortex.vortexdb.version;

import com.vortex.common.util.VersionUtil;
import com.vortex.common.util.VersionUtil.Version;
import com.vortex.common.version.CommonVersion;

public class CoreVersion {

    static {
        // Check versions of the dependency packages
        CoreVersion.check();
    }

    public static final String NAME = "vortex-core";

    // The second parameter of Version.of() is for IDE running without JAR
    public static final Version VERSION = Version.of(CoreVersion.class,
                                                     "0.12.0");

    public static final String GREMLIN_VERSION = "3.4.3";

    public static void check() {
        // Check version of vortex-common
        VersionUtil.check(CommonVersion.VERSION, "2.0.0", "2.1",
                          CommonVersion.NAME);
    }
}
