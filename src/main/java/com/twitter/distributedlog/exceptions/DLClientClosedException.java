package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

public class DLClientClosedException extends DLException {

    private static final long serialVersionUID = -8876218750540927584L;

    public DLClientClosedException(String msg) {
        super(StatusCode.CLIENT_CLOSED, msg);
    }

    public DLClientClosedException(String msg, Throwable t) {
        super(StatusCode.CLIENT_CLOSED, msg, t);
    }
}
