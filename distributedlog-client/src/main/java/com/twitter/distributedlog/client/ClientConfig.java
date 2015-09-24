package com.twitter.distributedlog.client;

import com.google.common.base.Preconditions;

import java.util.concurrent.TimeUnit;

/**
 * Client Config
 */
public class ClientConfig {
    int redirectBackoffStartMs = 25;
    int redirectBackoffMaxMs = 100;
    int maxRedirects = -1;
    int requestTimeoutMs = -1;
    boolean thriftmux = false;
    boolean streamFailfast = false;
    String streamNameRegex = ".*";
    boolean handshakeWithClientInfo = true;
    long periodicHandshakeIntervalMs = TimeUnit.MINUTES.toMillis(5);
    long periodicOwnershipSyncIntervalMs = TimeUnit.MINUTES.toMillis(5);
    boolean periodicDumpOwnershipCacheEnabled = false;
    long periodicDumpOwnershipCacheIntervalMs = TimeUnit.MINUTES.toMillis(10);
    boolean enableHandshakeTracing = false;
    boolean enableChecksum = true;

    public ClientConfig setMaxRedirects(int maxRedirects) {
        this.maxRedirects = maxRedirects;
        return this;
    }

    public int getMaxRedirects() {
        return this.maxRedirects;
    }

    public ClientConfig setRequestTimeoutMs(int timeoutInMillis) {
        this.requestTimeoutMs = timeoutInMillis;
        return this;
    }

    public int getRequestTimeoutMs() {
        return this.requestTimeoutMs;
    }

    public ClientConfig setRedirectBackoffStartMs(int ms) {
        this.redirectBackoffStartMs = ms;
        return this;
    }

    public int getRedirectBackoffStartMs() {
        return this.redirectBackoffStartMs;
    }

    public ClientConfig setRedirectBackoffMaxMs(int ms) {
        this.redirectBackoffMaxMs = ms;
        return this;
    }

    public int getRedirectBackoffMaxMs() {
        return this.redirectBackoffMaxMs;
    }

    public ClientConfig setThriftMux(boolean enabled) {
        this.thriftmux = enabled;
        return this;
    }

    public boolean getThriftMux() {
        return this.thriftmux;
    }

    public ClientConfig setStreamFailfast(boolean enabled) {
        this.streamFailfast = enabled;
        return this;
    }

    public boolean getStreamFailfast() {
        return this.streamFailfast;
    }

    public ClientConfig setStreamNameRegex(String nameRegex) {
        Preconditions.checkNotNull(nameRegex);
        this.streamNameRegex = nameRegex;
        return this;
    }

    public String getStreamNameRegex() {
        return this.streamNameRegex;
    }

    public ClientConfig setHandshakeWithClientInfo(boolean enabled) {
        this.handshakeWithClientInfo = enabled;
        return this;
    }

    public boolean getHandshakeWithClientInfo() {
        return this.handshakeWithClientInfo;
    }

    public ClientConfig setPeriodicHandshakeIntervalMs(long intervalMs) {
        this.periodicHandshakeIntervalMs = intervalMs;
        return this;
    }

    public long getPeriodicHandshakeIntervalMs() {
        return this.periodicHandshakeIntervalMs;
    }

    public ClientConfig setPeriodicOwnershipSyncIntervalMs(long intervalMs) {
        this.periodicOwnershipSyncIntervalMs = intervalMs;
        return this;
    }

    public long getPeriodicOwnershipSyncIntervalMs() {
        return this.periodicOwnershipSyncIntervalMs;
    }

    public ClientConfig setPeriodicDumpOwnershipCacheEnabled(boolean enabled) {
        this.periodicDumpOwnershipCacheEnabled = enabled;
        return this;
    }

    public boolean isPeriodicDumpOwnershipCacheEnabled() {
        return this.periodicDumpOwnershipCacheEnabled;
    }

    public ClientConfig setPeriodicDumpOwnershipCacheIntervalMs(long intervalMs) {
        this.periodicDumpOwnershipCacheIntervalMs = intervalMs;
        return this;
    }

    public long getPeriodicDumpOwnershipCacheIntervalMs() {
        return this.periodicDumpOwnershipCacheIntervalMs;
    }

    public ClientConfig setHandshakeTracingEnabled(boolean enabled) {
        this.enableHandshakeTracing = enabled;
        return this;
    }

    public boolean isHandshakeTracingEnabled() {
        return this.enableHandshakeTracing;
    }

    public ClientConfig setChecksumEnabled(boolean enabled) {
        this.enableChecksum = enabled;
        return this;
    }

    public boolean isChecksumEnabled() {
        return this.enableChecksum;
    }

    public static ClientConfig newConfig(ClientConfig config) {
        ClientConfig newConfig = new ClientConfig();
        newConfig.setMaxRedirects(config.getMaxRedirects())
                 .setRequestTimeoutMs(config.getRequestTimeoutMs())
                 .setRedirectBackoffStartMs(config.getRedirectBackoffStartMs())
                 .setRedirectBackoffMaxMs(config.getRedirectBackoffMaxMs())
                 .setThriftMux(config.getThriftMux())
                 .setStreamFailfast(config.getStreamFailfast())
                 .setStreamNameRegex(config.getStreamNameRegex())
                 .setHandshakeWithClientInfo(config.getHandshakeWithClientInfo())
                 .setPeriodicHandshakeIntervalMs(config.getPeriodicHandshakeIntervalMs())
                 .setPeriodicDumpOwnershipCacheEnabled(config.isPeriodicDumpOwnershipCacheEnabled())
                 .setPeriodicDumpOwnershipCacheIntervalMs(config.getPeriodicDumpOwnershipCacheIntervalMs())
                 .setHandshakeTracingEnabled(config.isHandshakeTracingEnabled())
                 .setChecksumEnabled(config.isChecksumEnabled());
        return newConfig;
    }
}
