
package com.vortex.common.license;

import org.apache.commons.lang.NotImplementedException;

public class LicenseManagerFactory {

    public static LicenseManager create(LicenseInstallParam param,
                                        LicenseManager.VerifyCallback veryfyCallback) {
        throw new NotImplementedException("No LicenseManager available");
    }
}
