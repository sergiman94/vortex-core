
package com.vortex.api.license;

import com.vortex.common.config.VortexConfig;
import com.vortex.common.license.LicenseInstallParam;
import com.vortex.common.license.LicenseManager;
import com.vortex.common.license.LicenseParams;
import com.vortex.vortexdb.VortexException;
import com.vortex.vortexdb.Vortex;
import com.vortex.common.config.VortexConfig;
import com.vortex.api.core.GraphManager;
import com.vortex.common.util.Log;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;

public class LicenseVerifier {

    private static final Logger LOG = Log.logger(Vortex.class);

    private static final String LICENSE_PARAM_PATH = "/verify-license.json";

    private static volatile LicenseVerifier INSTANCE = null;

    private static final Duration CHECK_INTERVAL = Duration.ofMinutes(10);
    private volatile Instant lastCheckTime = Instant.now();

    private final LicenseInstallParam installParam;
    private final LicenseVerifyManager manager;

    private LicenseVerifier() {
        this.installParam = buildInstallParam(LICENSE_PARAM_PATH);
        this.manager = new LicenseVerifyManager(this.installParam);
    }

    public static LicenseVerifier instance() {
        if (INSTANCE == null) {
            synchronized(LicenseVerifier.class) {
                if (INSTANCE == null) {
                    INSTANCE = new LicenseVerifier();
                }
            }
        }
        return INSTANCE;
    }

    public synchronized void install(VortexConfig config,
                                     GraphManager graphManager,
                                     String md5) {
        this.manager.config(config);
        this.manager.graphManager(graphManager);
        LicenseManager licenseManager = this.manager.licenseManager();
        try {
            licenseManager.uninstallLicense();
            this.verifyPublicCert(md5);
            LicenseParams params = licenseManager.installLicense();
            LOG.info("The license '{}' is successfully installed for '{}', " +
                     "the term of validity is from {} to {}",
                     params.subject(), params.consumerType(),
                     params.notBefore(), params.notAfter());
        } catch (Exception e) {
            LOG.error("Failed to install license", e);
            throw new VortexException("Failed to install license", e);
        }
    }

    public void verifyIfNeeded() {
        Instant now = Instant.now();
        Duration interval = Duration.between(this.lastCheckTime, now);
        if (!interval.minus(CHECK_INTERVAL).isNegative()) {
            this.verify();
            this.lastCheckTime = now;
        }
    }

    public void verify() {
        try {
            LicenseParams params = this.manager.licenseManager()
                                               .verifyLicense();
            LOG.info("The license verification passed, " +
                     "the term of validity is from {} to {}",
                     params.notBefore(), params.notAfter());
        } catch (Exception e) {
            LOG.error("Failed to verify license", e);
            throw new VortexException("Failed to verify license", e);
        }
    }

    private void verifyPublicCert(String expectMD5) {
        String path = this.installParam.publicKeyPath();
        try (InputStream is = LicenseVerifier.class.getResourceAsStream(path)) {
            String actualMD5 = DigestUtils.md5Hex(is);
            if (!actualMD5.equals(expectMD5)) {
                throw new VortexException("Invalid public cert");
            }
        } catch (IOException e) {
            LOG.error("Failed to read public cert", e);
            throw new VortexException("Failed to read public cert", e);
        }
    }

    private static LicenseInstallParam buildInstallParam(String path) {
        // NOTE: can't use JsonUtil due to it bind tinkerpop jackson
        ObjectMapper mapper = new ObjectMapper();
        try (InputStream stream =
             LicenseVerifier.class.getResourceAsStream(path)) {
            return mapper.readValue(stream, LicenseInstallParam.class);
        } catch (IOException e) {
            throw new VortexException("Failed to read json stream to %s",
                                    LicenseInstallParam.class);
        }
    }
}
