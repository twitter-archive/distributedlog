package com.twitter.distributedlog.feature;

/**
 * List of feature keys used by distributedlog core
 */
public enum CoreFeatureKeys {
    // @Deprecated: bkc features are managed by bookkeeper prefixed with a scope
    DISABLE_DURABILITY_ENFORCEMENT,
    // disabling logsegment rolling
    DISABLE_LOGSEGMENT_ROLLING,
    DISABLE_WRITE_LIMIT,
}