package com.twitter.distributedlog.lock;

/**
 * Lock Action
 */
interface LockAction {

    /**
     * Execute a lock action
     */
    void execute();

    /**
     * Get lock action name.
     *
     * @return lock action name
     */
    String getActionName();
}
