
package com.vortex.common.license;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.*;
import java.util.stream.Collectors;

public class MachineInfo {

    private List<String> ipAddressList;
    private List<String> macAddressList;

    public MachineInfo() {
        this.ipAddressList = null;
        this.macAddressList = null;
    }

    public List<String> getIpAddress() {
        if (this.ipAddressList != null) {
            return this.ipAddressList;
        }
        this.ipAddressList = new ArrayList<>();
        List<InetAddress> inetAddresses = this.getLocalAllInetAddress();
        if (inetAddresses != null && !inetAddresses.isEmpty()) {
            this.ipAddressList = inetAddresses.stream()
                                              .map(InetAddress::getHostAddress)
                                              .distinct()
                                              .map(String::toLowerCase)
                                              .collect(Collectors.toList());
        }
        return this.ipAddressList;
    }

    public List<String> getMacAddress() {
        if (this.macAddressList != null) {
            return this.macAddressList;
        }
        this.macAddressList = new ArrayList<>();
        List<InetAddress> inetAddresses = this.getLocalAllInetAddress();
        if (inetAddresses != null && !inetAddresses.isEmpty()) {
            // Get the Mac address of all network interfaces
            List<String> list = new ArrayList<>();
            Set<String> uniqueValues = new HashSet<>();
            for (InetAddress inetAddress : inetAddresses) {
                String macByInetAddress = this.getMacByInetAddress(inetAddress);
                if (uniqueValues.add(macByInetAddress)) {
                    list.add(macByInetAddress);
                }
            }
            this.macAddressList = list;
        }
        return this.macAddressList;
    }

    public List<InetAddress> getLocalAllInetAddress() {
        Enumeration<NetworkInterface> interfaces;
        try {
            interfaces = NetworkInterface.getNetworkInterfaces();
        } catch (SocketException e) {
            throw new RuntimeException("Failed to get network interfaces");
        }

        List<InetAddress> result = new ArrayList<>();
        while (interfaces.hasMoreElements()) {
            NetworkInterface nw = interfaces.nextElement();
            for (Enumeration<InetAddress> inetAddresses = nw.getInetAddresses();
                 inetAddresses.hasMoreElements(); ) {
                InetAddress inetAddr = inetAddresses.nextElement();
                if (!inetAddr.isLoopbackAddress() &&
                    !inetAddr.isLinkLocalAddress() &&
                    !inetAddr.isMulticastAddress()) {
                    result.add(inetAddr);
                }
            }
        }
        return result;
    }

    public String getMacByInetAddress(InetAddress inetAddr) {
        byte[] mac;
        try {
            mac = NetworkInterface.getByInetAddress(inetAddr)
                                  .getHardwareAddress();
        } catch (Exception e) {
            throw new RuntimeException(String.format(
                      "Failed to get mac address for inet address '%s'",
                      inetAddr));
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < mac.length; i++) {
            if (i != 0) {
                sb.append("-");
            }
            String temp = Integer.toHexString(mac[i] & 0xff);
            if (temp.length() == 1) {
                sb.append("0").append(temp);
            } else {
                sb.append(temp);
            }
        }
        return sb.toString().toUpperCase();
    }
}
