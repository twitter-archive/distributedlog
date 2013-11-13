package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

public class InternalServerException extends DLException {

    public InternalServerException(String msg) {
        super(StatusCode.INTERNAL_SERVER_ERROR, msg);
    }

    public InternalServerException(Throwable t) {
        super(StatusCode.INTERNAL_SERVER_ERROR, t);
    }

    public InternalServerException(String msg, Throwable t) {
        super(StatusCode.INTERNAL_SERVER_ERROR, msg, t);
    }
}
