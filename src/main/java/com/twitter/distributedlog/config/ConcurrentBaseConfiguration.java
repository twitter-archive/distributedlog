package com.twitter.distributedlog.config;

import com.google.common.base.Preconditions;

import org.apache.commons.configuration.AbstractConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Configuration view built on concurrent hash map for fast thread-safe access.
 * Notes:
 * 1. Multi-property list aggregation will not work in this class. I.e. commons config
 * normally combines all properties with the same key into one list property automatically.
 * This class simply overwrites any existing mapping.
 */
public class ConcurrentBaseConfiguration extends AbstractConfiguration {
    static final Logger LOG = LoggerFactory.getLogger(ConcurrentBaseConfiguration.class);

    private final ConcurrentHashMap<String, Object> map;

    public ConcurrentBaseConfiguration() {
        this.map = new ConcurrentHashMap<String, Object>();
    }

    @Override
    protected void addPropertyDirect(String key, Object value) {
        Preconditions.checkNotNull(value);
        map.put(key, value);
    }

    @Override
    public Object getProperty(String key) {
        return map.get(key);
    }

    @Override
    public Iterator getKeys() {
        return map.keySet().iterator();
    }

    @Override
    public boolean containsKey(String key) {
        return map.containsKey(key);
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    protected void clearPropertyDirect(String key) {
        map.remove(key);
    }
}
