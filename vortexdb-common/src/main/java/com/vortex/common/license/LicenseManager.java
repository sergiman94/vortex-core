
package com.vortex.common.license;

public interface LicenseManager {

    public LicenseParams installLicense() throws Exception;

    public void uninstallLicense() throws Exception;

    public LicenseParams verifyLicense() throws Exception;

    public interface VerifyCallback {

        public void onVerifyLicense(LicenseParams params) throws Exception;
    }
}
