package com.vortex.common.license;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.time.DateUtils;

import java.util.Date;
import java.util.List;

public class LicenseCommonParam {

    @JsonProperty("subject")
    private String subject;

    @JsonProperty("issued_time")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date issuedTime = new Date();

    @JsonProperty("not_before")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date notBefore = this.issuedTime;

    @JsonProperty("not_after")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date notAfter = DateUtils.addDays(this.notBefore, 30);

    @JsonProperty("consumer_type")
    private String consumerType = "user";

    @JsonProperty("consumer_amount")
    private Integer consumerAmount = 1;

    @JsonProperty("description")
    private String description = "";

    @JsonProperty("extra_params")
    private List<LicenseExtraParam> extraParams;

    public LicenseCommonParam() {
        // pass
    }

    public LicenseCommonParam(String subject, String description,
                              Date issued, Date notBefore, Date notAfter,
                              String consumerType, int consumerAmount,
                              List<LicenseExtraParam> extraParams) {
        this.subject = subject;
        this.description = description;
        this.issuedTime = issued;
        this.notBefore = notBefore;
        this.notAfter = notAfter;
        this.consumerType = consumerType;
        this.consumerAmount = consumerAmount;
        this.extraParams = extraParams;
    }

    public String subject() {
        return this.subject;
    }

    public Date issuedTime() {
        return this.issuedTime;
    }

    public Date notBefore() {
        return this.notBefore;
    }

    public Date notAfter() {
        return this.notAfter;
    }

    public String consumerType() {
        return this.consumerType;
    }

    public Integer consumerAmount() {
        return this.consumerAmount;
    }

    public String description() {
        return this.description;
    }

    public List<LicenseExtraParam> extraParams() {
        return this.extraParams;
    }
}
