package com.twitter.distributedlog.net;

import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TODO: we might need to move a twitter specific module?
 */
public class TwitterDNSResolver implements DNSToSwitchMapping {
    static final Logger LOG = LoggerFactory.getLogger(TwitterDNSResolver.class);

    static final String DEFAULT_RACK = "/default-region/default-rack";

    protected final ConcurrentMap<String, String> domainName2Racks =
            new ConcurrentHashMap<String, String>();

    protected final ConcurrentMap<String, String> hostNameToRegion =
        new ConcurrentHashMap<String, String>();

    public TwitterDNSResolver(String hostRegionOverrides) {
        // Host Region Overrides are of the form
        // HN1:R1;HN2:R2;...
        String[] overrides = hostRegionOverrides.split(";");

        for(String override: overrides) {
            String[] parts = override.split(":");
            if (parts.length != 2) {
                LOG.warn("Incorrect override specified", override);
            } else {
                hostNameToRegion.putIfAbsent(parts[0], parts[1]);
            }
        }
    }


    @Override
    public List<String> resolve(List<String> names) {
        List<String> racks = new ArrayList<String>(names.size());
        for (String name : names) {
            racks.add(resolve(name));
        }
        return racks;
    }

    private String resolve(String domainName) {
        String rack = domainName2Racks.get(domainName);
        if (null == rack) {
            rack = doResolve(domainName);
            domainName2Racks.put(domainName, rack);
        }
        return rack;
    }

    private String doResolve(String domainName) {
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

    @Override
    public void reloadCachedMappings() {
        domainName2Racks.clear();
    }
}
