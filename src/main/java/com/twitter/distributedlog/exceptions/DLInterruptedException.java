package com.twitter.distributedlog.exceptions;

import java.io.IOException;

/**
 * An interrupted exception wrapper indicates dl operations are interrupted.
 */
public class DLInterruptedException extends IOException {

    public DLInterruptedException(String msg) {
        super(msg);
    }

    public DLInterruptedException(String msg, Throwable t) {
        super(msg, t);
    }
}
