package com.twitter.distributedlog.net;

import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Abstract DNS resolver for bookkeeper ensemble placement.
 */
public abstract class DNSResolver implements DNSToSwitchMapping {
    static final Logger LOG = LoggerFactory.getLogger(DNSResolver.class);

    protected final ConcurrentMap<String, String> domainNameToNetworkLocation =
            new ConcurrentHashMap<String, String>();

    protected final ConcurrentMap<String, String> hostNameToRegion =
        new ConcurrentHashMap<String, String>();

    /**
     * Construct the default dns resolver without host-region overrides.
     */
    public DNSResolver() {
        this("");
    }

    /**
     * Construct the dns resolver with host-region overrides.
     * <p>
     * <i>hostRegionOverrides</i> is a string of pairs of host-region mapping
     * (host:region) separated by ';'. during dns resolution, the host will be resolved
     * to override region. example: <i>host1:region1;host2:region2;...</i>
     *
     * @param hostRegionOverrides
     *          pairs of host-region mapping separated by ';'
     */
    public DNSResolver(String hostRegionOverrides) {
        if (StringUtils.isNotBlank(hostRegionOverrides)) {
            // Host Region Overrides are of the form
            // HN1:R1;HN2:R2;...
            String[] overrides = hostRegionOverrides.split(";");

            for (String override : overrides) {
                String[] parts = override.split(":");
                if (parts.length != 2) {
                    LOG.warn("Incorrect override specified", override);
                } else {
                    hostNameToRegion.putIfAbsent(parts[0], parts[1]);
                }
            }
        } // otherwise, no overrides were specified
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> resolve(List<String> names) {
        List<String> networkLocations = new ArrayList<String>(names.size());
        for (String name : names) {
            networkLocations.add(resolve(name));
        }
        return networkLocations;
    }

    private String resolve(String domainName) {
        String networkLocation = domainNameToNetworkLocation.get(domainName);
        if (null == networkLocation) {
            networkLocation = resolveToNetworkLocation(domainName);
            domainNameToNetworkLocation.put(domainName, networkLocation);
        }
        return networkLocation;
    }

    /**
     * Resolve the <code>domainName</code> to its network location.
     *
     * @param domainName
     *          domain name
     * @return the network location of <i>domainName</i>
     */
    protected abstract String resolveToNetworkLocation(String domainName);

    /**
     * {@inheritDoc}
     */
    @Override
    public void reloadCachedMappings() {
        domainNameToNetworkLocation.clear();
    }
}
