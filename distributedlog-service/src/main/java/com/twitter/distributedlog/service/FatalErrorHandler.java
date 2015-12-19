package com.twitter.distributedlog.service;

import com.google.common.base.Optional;
import com.twitter.util.Future;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implement handling for an unrecoverable error.
 */
public interface FatalErrorHandler {

    /**
     * This method is invoked when an unrecoverable error has occurred
     * and no progress can be made. It should implement a shutdown routine.
     */
    void notifyFatalError();
}