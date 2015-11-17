package com.twitter.distributedlog.v2.net;

import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TODO: we might need to move a twitter specific module?
 */
public abstract class TwitterDNSResolver implements DNSToSwitchMapping {
    static final Logger LOG = LoggerFactory.getLogger(TwitterDNSResolver.class);

    protected final ConcurrentMap<String, String> domainNameToNetworkLocation =
            new ConcurrentHashMap<String, String>();

    protected final ConcurrentMap<String, String> hostNameToRegion =
        new ConcurrentHashMap<String, String>();

    public TwitterDNSResolver() {
        this("");
    }

    public TwitterDNSResolver(String hostRegionOverrides) {
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

    protected abstract String resolveToNetworkLocation(String domainName);

    @Override
    public void reloadCachedMappings() {
        domainNameToNetworkLocation.clear();
    }
}
