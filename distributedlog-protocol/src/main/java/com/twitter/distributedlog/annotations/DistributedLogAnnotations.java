package com.twitter.distributedlog.annotations;

public class DistributedLogAnnotations {
    /**
     * Annotation to identify flaky tests in DistributedLog.
     * As and when we find that a test is flaky, we'll add this annotation to it for reference.
     */
    public @interface FlakyTest {}

    /**
     * Annotation to specify the occurrence of a compression operation. These are CPU intensive
     * and should be avoided in low-latency paths.
     */
    public @interface Compression {}
}
