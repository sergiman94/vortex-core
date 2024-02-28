

package com.vortex.common.license;

import java.util.Date;
import java.util.List;

public class LicenseParams extends LicenseCommonParam {

    public LicenseParams() {
        super();
    }

    public LicenseParams(String subject, String description,
                         Date issued, Date notBefore, Date notAfter,
                         String consumerType, int consumerAmount,
                         List<LicenseExtraParam> extraParams) {
        super(subject, description, issued, notBefore, notAfter,
              consumerType, consumerAmount, extraParams);
    }

    public LicenseExtraParam matchParam(String id) {
        for (LicenseExtraParam param : this.extraParams()) {
            if (param.id().equals(id)) {
                return param;
            }
        }
        return null;
    }
}
