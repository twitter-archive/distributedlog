package com.twitter.distributedlog.lock;

/**
 * Listener on lock state changes
 */
interface LockListener {
    /**
     * Triggered when a lock is changed from CLAIMED to EXPIRED.
     */
    void onExpired();
}
