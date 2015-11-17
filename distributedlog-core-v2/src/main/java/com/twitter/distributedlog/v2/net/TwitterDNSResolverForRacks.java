package com.twitter.distributedlog.v2.net;

public class TwitterDNSResolverForRacks extends TwitterDNSResolver {
    static final String DEFAULT_RACK = "/default-region/default-rack";

    public TwitterDNSResolverForRacks() {
    }

    public TwitterDNSResolverForRacks(String hostRegionOverrides) {
        super(hostRegionOverrides);
    }

    @Override
    protected String resolveToNetworkLocation(String domainName) {
        String[] parts = domainName.split("\\.");
        if (parts.length <= 0) {
            return DEFAULT_RACK;
        }

        String hostName = parts[0];
        String[] labels = hostName.split("-");
        if (labels.length != 4) {
            return DEFAULT_RACK;
        }

        String region = hostNameToRegion.get(hostName);
        if (null == region) {
            region = labels[0];
        }

        return String.format("/%s/%s", region, labels[1]);
    }
}
