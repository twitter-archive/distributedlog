package com.twitter.distributedlog.client;

import com.google.common.base.Preconditions;

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
    boolean handshakeWithClientInfo = false;

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

    public static ClientConfig newConfig(ClientConfig config) {
        ClientConfig newConfig = new ClientConfig();
        newConfig.setMaxRedirects(config.getMaxRedirects())
                 .setRequestTimeoutMs(config.getRequestTimeoutMs())
                 .setRedirectBackoffStartMs(config.getRedirectBackoffStartMs())
                 .setRedirectBackoffMaxMs(config.getRedirectBackoffMaxMs())
                 .setThriftMux(config.getThriftMux())
                 .setStreamFailfast(config.getStreamFailfast())
                 .setStreamNameRegex(config.getStreamNameRegex())
                 .setHandshakeWithClientInfo(config.getHandshakeWithClientInfo());
        return newConfig;
    }
}
