package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

public class UnexpectedException extends DLException {

    private static final long serialVersionUID = 903763128422774055L;

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
