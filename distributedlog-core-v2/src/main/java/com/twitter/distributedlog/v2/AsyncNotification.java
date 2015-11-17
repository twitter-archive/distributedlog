package com.twitter.distributedlog.v2;

interface AsyncNotification {
    /**
     * Triggered when the background activity encounters an exception
     */
    void notifyOnError();

    /**
     *  Triggered when the background activity completes an operation
     */
    void notifyOnOperationComplete();
}
