package com.twitter.distributedlog.service;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

class TwitterRegionResolver implements RegionResolver {

    private static final String DEFAULT_REGION = "default-region";

    private final Map<SocketAddress, String> regionOverrides =
            new HashMap<SocketAddress, String>();
    private final ConcurrentMap<SocketAddress, String> regionMap =
            new ConcurrentHashMap<SocketAddress, String>();

    TwitterRegionResolver() {
    }

    TwitterRegionResolver(Map<SocketAddress, String> regionOverrides) {
        this.regionOverrides.putAll(regionOverrides);
    }

    @Override
    public String resolveRegion(SocketAddress address) {
        String region = regionMap.get(address);
        if (null == region) {
            region = doResolveRegion(address);
            regionMap.put(address, region);
        }
        return region;
    }

    private String doResolveRegion(SocketAddress address) {
        String region = regionOverrides.get(address);
        if (null != region) {
            return region;
        }

        String domainName;
        if (address instanceof InetSocketAddress) {
            InetSocketAddress iAddr = (InetSocketAddress) address;
            domainName = iAddr.getHostName();
        } else {
            domainName = address.toString();
        }
        String[] parts = domainName.split("\\.");
        if (parts.length <= 0) {
            return DEFAULT_REGION;
        }
        String hostName = parts[0];
        String[] labels = hostName.split("-");
        if (labels.length != 4) {
            return DEFAULT_REGION;
        }
        return labels[0];
    }

    @Override
    public void removeCachedHost(SocketAddress address) {
        regionMap.remove(address);
    }
}
