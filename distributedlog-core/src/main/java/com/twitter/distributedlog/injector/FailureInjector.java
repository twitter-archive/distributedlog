package com.twitter.distributedlog.injector;

/**
 * Failure injector.
 */
public interface FailureInjector {

    /**
     * No-op failure injector, which does nothing.
     */
    public static FailureInjector NULL = new FailureInjector() {
        @Override
        public void inject() {
            // no-op;
        }
    };

    // inject failures
    void inject();
}
