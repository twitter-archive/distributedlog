package com.twitter.distributedlog;

import com.twitter.distributedlog.exceptions.DLException;
import com.twitter.distributedlog.thrift.service.StatusCode;

public class FlushException extends DLException {

    private static final long serialVersionUID = -9060360360261130489L;

    public FlushException(String message) {
        super(StatusCode.FLUSH_TIMEOUT, message);
    }

    public FlushException(String message, Throwable cause) {
        super(StatusCode.FLUSH_TIMEOUT, message, cause);
    }
}
