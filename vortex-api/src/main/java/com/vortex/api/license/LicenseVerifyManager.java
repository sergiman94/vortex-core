
package com.vortex.api.license;

import com.vortex.common.license.*;
import com.vortex.vortexdb.VortexException;
import com.vortex.vortexdb.config.CoreOptions;
import com.vortex.common.config.VortexConfig;
import com.vortex.api.config.ServerOptions;
import com.vortex.api.core.GraphManager;
import com.vortex.common.util.Bytes;
import com.vortex.common.util.E;
import com.vortex.common.util.Log;
import com.vortex.common.util.VersionUtil;
import com.vortex.vortexdb.version.CoreVersion;
import com.sun.management.OperatingSystemMXBean;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

public class LicenseVerifyManager {

    private static final Logger LOG = Log.logger(LicenseVerifyManager.class);

    private final LicenseManager licenseManager;
    private final MachineInfo machineInfo;

    private VortexConfig config;
    private GraphManager graphManager;

    public LicenseVerifyManager(LicenseInstallParam param) {
        this.licenseManager = LicenseManagerFactory.create(param,
                                                           this::validate);
        this.machineInfo = new MachineInfo();
    }

    public LicenseManager licenseManager() {
        return this.licenseManager;
    }

    public void config(VortexConfig config) {
        this.config = config;
    }

    public VortexConfig config() {
        E.checkState(this.config != null,
                     "License verify manager has not been installed");
        return this.config;
    }

    public void graphManager(GraphManager graphManager) {
        this.graphManager = graphManager;
    }

    public GraphManager graphManager() {
        E.checkState(this.graphManager != null,
                     "License verify manager has not been installed");
        return this.graphManager;
    }

    private synchronized void validate(LicenseParams params) {
        // Verify the customized license parameters.
        String serverId = this.getServerId();
        LOG.debug("Verify server id '{}'", serverId);
        LicenseExtraParam param = params.matchParam(serverId);
        if (param == null) {
            throw new VortexException("The current server id is not authorized");
        }

        this.checkVersion(param);
        this.checkGraphs(param);
        this.checkIpAndMac(param);
        this.checkCpu(param);
        this.checkRam(param);
        this.checkThreads(param);
        this.checkMemory(param);
    }

    private String getServerId() {
        return this.config().get(ServerOptions.SERVER_ID);
    }

    private void checkVersion(LicenseExtraParam param) {
        String expectVersion = param.version();
        if (StringUtils.isEmpty(expectVersion)) {
            return;
        }
        VersionUtil.Version acutalVersion = CoreVersion.VERSION;
        if (acutalVersion.compareTo(VersionUtil.Version.of(expectVersion)) > 0) {
            throw newLicenseException(
                  "The server's version '%s' exceeded the authorized '%s'",
                  acutalVersion.get(), expectVersion);
        }
    }

    private void checkGraphs(LicenseExtraParam param) {
        int expectGraphs = param.graphs();
        if (expectGraphs == LicenseExtraParam.NO_LIMIT) {
            return;
        }
        int actualGraphs = this.graphManager().graphs().size();
        if (actualGraphs > expectGraphs) {
            throw newLicenseException(
                  "The server's graphs '%s' exceeded the authorized '%s'",
                  actualGraphs, expectGraphs);
        }
    }

    private void checkIpAndMac(LicenseExtraParam param) {
        String expectIp = param.ip();
        boolean matched = false;
        List<String> actualIps = this.machineInfo.getIpAddress();
        for (String actualIp : actualIps) {
            if (StringUtils.isEmpty(expectIp) ||
                actualIp.equalsIgnoreCase(expectIp)) {
                matched = true;
                break;
            }
        }
        if (!matched) {
            throw newLicenseException(
                  "The server's ip '%s' doesn't match the authorized '%s'",
                  actualIps, expectIp);
        }

        String expectMac = param.mac();
        if (StringUtils.isEmpty(expectMac)) {
            return;
        }

        // The mac must be not empty here
        if (!StringUtils.isEmpty(expectIp)) {
            String actualMac;
            try {
                actualMac = this.machineInfo.getMacByInetAddress(
                            InetAddress.getByName(expectIp));
            } catch (UnknownHostException e) {
                throw newLicenseException(
                      "Failed to get mac address for ip '%s'", expectIp);
            }
            String expectFormatMac = expectMac.replaceAll(":", "-");
            String actualFormatMac = actualMac.replaceAll(":", "-");
            if (!actualFormatMac.equalsIgnoreCase(expectFormatMac)) {
                throw newLicenseException(
                      "The server's mac '%s' doesn't match the authorized '%s'",
                      actualMac, expectMac);
            }
        } else {
            String expectFormatMac = expectMac.replaceAll(":", "-");
            List<String> actualMacs = this.machineInfo.getMacAddress();
            matched = false;
            for (String actualMac : actualMacs) {
                String actualFormatMac = actualMac.replaceAll(":", "-");
                if (actualFormatMac.equalsIgnoreCase(expectFormatMac)) {
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                throw newLicenseException(
                      "The server's macs %s don't match the authorized '%s'",
                      actualMacs, expectMac);
            }
        }
    }

    private void checkCpu(LicenseExtraParam param) {
        int expectCpus = param.cpus();
        if (expectCpus == LicenseExtraParam.NO_LIMIT) {
            return;
        }
        int actualCpus = CoreOptions.CPUS;
        if (actualCpus > expectCpus) {
            throw newLicenseException(
                  "The server's cpus '%s' exceeded the limit '%s'",
                  actualCpus, expectCpus);
        }
    }

    private void checkRam(LicenseExtraParam param) {
        // Unit MB
        int expectRam = param.ram();
        if (expectRam == LicenseExtraParam.NO_LIMIT) {
            return;
        }
        OperatingSystemMXBean mxBean = (OperatingSystemMXBean) ManagementFactory
                                       .getOperatingSystemMXBean();
        long actualRam = mxBean.getTotalPhysicalMemorySize() / Bytes.MB;
        if (actualRam > expectRam) {
            throw newLicenseException(
                  "The server's ram(MB) '%s' exceeded the limit(MB) '%s'",
                  actualRam, expectRam);
        }
    }

    private void checkThreads(LicenseExtraParam param) {
        int expectThreads = param.threads();
        if (expectThreads == LicenseExtraParam.NO_LIMIT) {
            return;
        }
        int actualThreads = this.config().get(ServerOptions.MAX_WORKER_THREADS);
        if (actualThreads > expectThreads) {
            throw newLicenseException(
                  "The server's max threads '%s' exceeded limit '%s'",
                  actualThreads, expectThreads);
        }
    }

    private void checkMemory(LicenseExtraParam param) {
        // Unit MB
        int expectMemory = param.memory();
        if (expectMemory == LicenseExtraParam.NO_LIMIT) {
            return;
        }
        /*
         * NOTE: this max memory will be slightly smaller than XMX,
         * because only one survivor will be used
         */
        long actualMemory = Runtime.getRuntime().maxMemory() / Bytes.MB;
        if (actualMemory > expectMemory) {
            throw newLicenseException(
                  "The server's max heap memory(MB) '%s' exceeded the " +
                  "limit(MB) '%s'", actualMemory, expectMemory);
        }
    }

    private VortexException newLicenseException(String message, Object... args) {
        return new VortexException(message, args);
    }
}
