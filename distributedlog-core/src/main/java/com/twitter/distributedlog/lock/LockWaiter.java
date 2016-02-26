package com.twitter.distributedlog.lock;

import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Future;
import com.twitter.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lock waiter represents the attempt that application tries to lock.
 */
public class LockWaiter {

    private static final Logger logger = LoggerFactory.getLogger(LockWaiter.class);

    private final String lockId;
    private final String currentOwner;
    private final Future<Boolean> acquireFuture;

    public LockWaiter(String lockId,
                      String currentOwner,
                      Future<Boolean> acquireFuture) {
        this.lockId = lockId;
        this.currentOwner = currentOwner;
        this.acquireFuture = acquireFuture;
    }

    /**
     * Return the lock id of the waiter.
     *
     * @return lock id of the waiter
     */
    public String getId() {
        return lockId;
    }

    /**
     * Return the owner that observed when waiter is waiting.
     *
     * @return the owner that observed when waiter is waiting
     */
    public String getCurrentOwner() {
        return currentOwner;
    }

    /**
     * Return the future representing the waiting result.
     *
     * <p>If the future is interrupted (e.g. {@link Future#within(Duration, Timer)}),
     * the waiter will automatically clean up its waiting state.
     *
     * @return the future representing the acquire result.
     */
    public Future<Boolean> getAcquireFuture() {
        return acquireFuture;
    }

    /**
     * Wait for the acquire result.
     *
     * @return true if acquired successfully, otherwise false.
     */
    public boolean waitForAcquireQuietly() {
        boolean success = false;
        try {
            success = Await.result(acquireFuture);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } catch (LockTimeoutException lte) {
            logger.debug("Timeout on lock acquiring", lte);
        } catch (Exception e) {
            logger.error("Caught exception waiting for lock acquired", e);
        }
        return success;
    }

}
