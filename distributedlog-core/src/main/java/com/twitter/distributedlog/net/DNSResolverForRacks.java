package com.twitter.distributedlog.net;

/**
 * Resolve the dns by racks.
 * <p>
 * It resolves domain name like `(region)-(rack)-xxx-xxx.*` to network location
 * `/(region)/(rack)`. If resolution failed, it returns `/default-region/default-rack`.
 * <p>
 * region could be override in <code>hostRegionOverrides</code>. for example, if the
 * host name is <i>regionA-rack1-xx-yyy</i>, it would be resolved to `/regionA/rack1`
 * without any overrides. If the specified overrides is <i>regionA-rack1-xx-yyy:regionB</i>,
 * the resolved network location would be <i>/regionB/rack1</i>.
 * <p>
 * Region overrides provide optimization hits to bookkeeper if two `logical` regions are
 * in same or close locations.
 *
 * @see DNSResolver#DNSResolver(String)
 */
public class DNSResolverForRacks extends DNSResolver {
    static final String DEFAULT_RACK = "/default-region/default-rack";

    public DNSResolverForRacks() {
    }

    public DNSResolverForRacks(String hostRegionOverrides) {
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
