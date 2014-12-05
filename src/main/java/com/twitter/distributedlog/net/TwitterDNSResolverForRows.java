package com.twitter.distributedlog.net;

public class TwitterDNSResolverForRows extends TwitterDNSResolver {
    static final String DEFAULT_ROW = "/default-region/default-row";

    public TwitterDNSResolverForRows() {
    }

    public TwitterDNSResolverForRows(String hostRegionOverrides) {
        super(hostRegionOverrides);
    }

    @Override
    protected String resolveToNetworkLocation(String domainName) {
        String[] parts = domainName.split("\\.");
        if (parts.length <= 0) {
            return DEFAULT_ROW;
        }
        String hostName = parts[0];
        String[] labels = hostName.split("-");
        if (labels.length != 4) {
            return DEFAULT_ROW;
        }

        String region = hostNameToRegion.get(hostName);
        if (null == region) {
            region = labels[0];
        }

        final String rack = labels[1];

        if (rack.length() < 2) {
            // Default to rack name if the rack name format cannot be recognized
            return String.format("/%s/%s", region, rack);
        } else {
            return String.format("/%s/%s", region, rack.substring(0, rack.length() - 1));
        }
    }
}
