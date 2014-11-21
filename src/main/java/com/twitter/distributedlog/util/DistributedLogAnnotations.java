package com.twitter.distributedlog.util;

public class DistributedLogAnnotations {
    /**
     * Annotation to identify flaky tests in DistributedLog.
     * As and when we find that a test is flaky, we'll add this annotation to it for reference.
     */
    public @interface FlakyTest {}
}
