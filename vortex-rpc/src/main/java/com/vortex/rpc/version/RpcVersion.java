
package com.vortex.rpc.version;

import com.vortex.common.util.VersionUtil.Version;

public class RpcVersion {

    public static final String NAME = "vortex-rpc";

    // The second parameter of Version.of() is for all-in-one JAR
    public static final Version VERSION = Version.of(RpcVersion.class,
                                                     "2.1.0");
}
