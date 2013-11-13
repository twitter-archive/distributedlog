package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

public class UnexpectedException extends DLException {
    public UnexpectedException() {
        super(StatusCode.UNEXPECTED);
    }
}
