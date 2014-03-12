package com.twitter.distributedlog;

import com.twitter.distributedlog.exceptions.DLException;
import com.twitter.distributedlog.thrift.service.StatusCode;

/**
 * Thrown when the distributed log manager has already been closed
 * (connections have been torn down)
 */
public class AlreadyClosedException extends DLException {

    private static final long serialVersionUID = -4721864322739563725L;

    public AlreadyClosedException(String message) {
        super(StatusCode.ALREADY_CLOSED, message);
    }
}
