package com.twitter.distributedlog.net;

import org.apache.bookkeeper.net.DNSToSwitchMapping;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TODO: we might need to move a twitter specific module?
 */
public class TwitterDNSResolver implements DNSToSwitchMapping {

    static final String DEFAULT_RACK = "/default-region/default-rack";

    protected final ConcurrentMap<String, String> domainName2Racks =
            new ConcurrentHashMap<String, String>();

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
        String[] labels = parts[0].split("-");
        if (labels.length != 4) {
            return DEFAULT_RACK;
        }
        return String.format("/%s/%s", labels[0], labels[1]);
    }

    @Override
    public void reloadCachedMappings() {
        domainName2Racks.clear();
    }
}
