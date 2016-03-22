package com.twitter.distributedlog.injector;

import com.twitter.distributedlog.util.Utils;

import java.util.Random;

/**
 * Failure injector based on {@link java.util.Random}
 */
public class AsyncRandomFailureInjector implements AsyncFailureInjector {

    private static final Random random = new Random(System.currentTimeMillis());

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {

        private boolean _simulateDelays = false;
        private boolean _simulateErrors = false;
        private boolean _simulateStops = false;
        private boolean _simulateCorruption = false;
        private int _injectedDelayPercent = 0;
        private int _injectedErrorPercent = 0;
        private int _injectedStopPercent = 0;
        private int _maxInjectedDelayMs = Integer.MAX_VALUE;

        private Builder() {}

        public Builder injectDelays(boolean simulateDelays,
                                    int injectedDelayPercent,
                                    int maxInjectedDelayMs) {
            this._simulateDelays = simulateDelays;
            this._injectedDelayPercent = injectedDelayPercent;
            this._maxInjectedDelayMs = maxInjectedDelayMs;
            return this;
        }

        public Builder injectErrors(boolean simulateErrors,
                                    int injectedErrorPercent) {
            this._simulateErrors = simulateErrors;
            this._injectedErrorPercent = injectedErrorPercent;
            return this;
        }

        public Builder injectCorruption(boolean simulateCorruption) {
            this._simulateCorruption = simulateCorruption;
            return this;
        }

        public Builder injectStops(boolean simulateStops,
                                   int injectedStopPercent) {
            this._simulateStops = simulateStops;
            this._injectedStopPercent = injectedStopPercent;
            return this;
        }

        public AsyncFailureInjector build() {
            return new AsyncRandomFailureInjector(
                    _simulateDelays,
                    _injectedDelayPercent,
                    _maxInjectedDelayMs,
                    _simulateErrors,
                    _injectedErrorPercent,
                    _simulateStops,
                    _injectedStopPercent,
                    _simulateCorruption);
        }

    }

    private boolean simulateDelays;
    private boolean simulateErrors;
    private boolean simulateStops;
    private boolean simulateCorruption;
    private final int injectedDelayPercent;
    private final int injectedErrorPercent;
    private final int injectedStopPercent;
    private final int maxInjectedDelayMs;

    private AsyncRandomFailureInjector(boolean simulateDelays,
                                       int injectedDelayPercent,
                                       int maxInjectedDelayMs,
                                       boolean simulateErrors,
                                       int injectedErrorPercent,
                                       boolean simulateStops,
                                       int injectedStopPercent,
                                       boolean simulateCorruption) {
        this.simulateDelays = simulateDelays;
        this.injectedDelayPercent = injectedDelayPercent;
        this.maxInjectedDelayMs = maxInjectedDelayMs;
        this.simulateErrors = simulateErrors;
        this.injectedErrorPercent = injectedErrorPercent;
        this.simulateStops = simulateStops;
        this.injectedStopPercent = injectedStopPercent;
        this.simulateCorruption = simulateCorruption;
    }

    @Override
    public void injectErrors(boolean enabled) {
        this.simulateErrors = enabled;
    }

    @Override
    public boolean shouldInjectErrors() {
        return simulateErrors && Utils.randomPercent(injectedErrorPercent);
    }

    @Override
    public void injectDelays(boolean enabled) {
        this.simulateDelays = enabled;
    }

    @Override
    public boolean shouldInjectDelays() {
        return simulateDelays && Utils.randomPercent(injectedDelayPercent);
    }

    @Override
    public int getInjectedDelayMs() {
        if (maxInjectedDelayMs > 0) {
            return random.nextInt(maxInjectedDelayMs);
        }
        return 0;
    }

    @Override
    public void injectStops(boolean enabled) {
        this.simulateStops = enabled;
    }

    @Override
    public boolean shouldInjectStops() {
        return simulateStops && Utils.randomPercent(injectedStopPercent);
    }

    @Override
    public boolean shouldInjectCorruption() {
        return simulateCorruption;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("FailureInjector[");
        sb.append("errors=(").append(simulateErrors).append(", pct=")
                .append(injectedErrorPercent).append("), ");
        sb.append("delays=(").append(simulateDelays).append(", pct=")
                .append(injectedDelayPercent).append(", max=")
                .append(maxInjectedDelayMs).append("), ");
        sb.append("stops=(").append(simulateStops).append(", pct=")
                .append(injectedStopPercent).append(")");
        sb.append("corruption=(").append(simulateCorruption).append(")");
        sb.append("]");
        return sb.toString();
    }
}
