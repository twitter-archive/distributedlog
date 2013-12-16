package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

public class UnexpectedException extends DLException {
    public UnexpectedException() {
        super(StatusCode.UNEXPECTED);
    }

    public UnexpectedException(String msg) {
        super(StatusCode.UNEXPECTED, msg);
    }

    public UnexpectedException(String msg, Throwable t) {
        super(StatusCode.UNEXPECTED, msg, t);
    }
}
