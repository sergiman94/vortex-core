
package com.vortex.common.version;

import com.vortex.common.util.VersionUtil.Version;

public class CommonVersion {

    public static final String NAME = "vortex-common";

    // The second parameter of Version.of() is for all-in-one JAR
    public static final Version VERSION = Version.of(CommonVersion.class,
                                                     "2.1.0");
}
