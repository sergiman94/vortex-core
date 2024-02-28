package com.vortex.client.version;

import com.vortex.common.util.VersionUtil;
import com.vortex.common.util.VersionUtil.Version;
import com.vortex.common.version.CommonVersion;

public class ClientVersion {

    static {
        // Check versions of the dependency packages
        ClientVersion.check();
    }

    public static final String NAME = "hugegraph-client";

    public static final Version VERSION = Version.of(ClientVersion.class);

    public static void check() {
        // Check version of vortex-common
        VersionUtil.check(CommonVersion.VERSION, "2.1", "2.2",
                          CommonVersion.NAME);
    }
}
