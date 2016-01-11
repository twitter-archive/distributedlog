package com.twitter.distributedlog.net;

/**
 * Resolve the dns by rows.
 * <p>
 * It resolves domain name like `(region)-(row)xx-xxx-xxx.*` to network location
 * `/(region)/(row)`. If resolution failed, it returns `/default-region/default-row`.
 * <p>
 * region could be override in <code>hostRegionOverrides</code>. for example, if the
 * host name is <i>regionA-row1-xx-yyy</i>, it would be resolved to `/regionA/row1`
 * without any overrides. If the specified overrides is <i>regionA-row1-xx-yyy:regionB</i>,
 * the resolved network location would be <i>/regionB/row1</i>.
 * <p>
 * Region overrides provide optimization hits to bookkeeper if two `logical` regions are
 * in same or close locations.
 *
 * @see DNSResolver#DNSResolver(String)
 */
public class DNSResolverForRows extends DNSResolver {
    static final String DEFAULT_ROW = "/default-region/default-row";

    public DNSResolverForRows() {
    }

    public DNSResolverForRows(String hostRegionOverrides) {
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
            return String.format("/%s/%s", region, rack.substring(0, 2));
        }
    }
}
