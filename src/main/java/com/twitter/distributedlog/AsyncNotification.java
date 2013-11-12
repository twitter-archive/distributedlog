package com.twitter.distributedlog;

public interface AsyncNotification {
    /**
     * Triggered when the background activity encounters an exception
     */
    void notifyOnError();

    /**
     *  Triggered when the background activity completes an operation
     */
    void notifyOnOperationComplete();
}
