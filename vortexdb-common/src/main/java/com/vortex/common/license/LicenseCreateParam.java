
package com.vortex.common.license;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;

public class LicenseCreateParam extends LicenseCommonParam {

    @JsonProperty("private_alias")
    private String privateAlias;

    @JsonAlias("key_ticket")
    @JsonProperty("key_password")
    private String keyPassword;

    @JsonAlias("store_ticket")
    @JsonProperty("store_password")
    private String storePassword;

    @JsonProperty("privatekey_path")
    private String privateKeyPath;

    @JsonProperty("license_path")
    private String licensePath;

    public String privateAlias() {
        return this.privateAlias;
    }

    public String keyPassword() {
        return this.keyPassword;
    }

    public String storePassword() {
        return this.storePassword;
    }

    public String privateKeyPath() {
        return this.privateKeyPath;
    }

    public String licensePath() {
        return this.licensePath;
    }
}
