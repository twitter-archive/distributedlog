package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

/**
 * An interrupted exception wrapper indicates dl operations are interrupted.
 */
public class DLInterruptedException extends DLException {

    public DLInterruptedException(String msg) {
        super(StatusCode.INTERRUPTED, msg);
    }

    public DLInterruptedException(String msg, Throwable t) {
        super(StatusCode.INTERRUPTED, msg, t);
    }
}
