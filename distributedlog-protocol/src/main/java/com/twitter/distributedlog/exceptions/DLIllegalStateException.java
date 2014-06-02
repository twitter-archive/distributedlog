package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

public class DLIllegalStateException extends DLException {

    private static final long serialVersionUID = -6721471104777747420L;

    public DLIllegalStateException(String msg) {
        super(StatusCode.ILLEGAL_STATE, msg);
    }

    public DLIllegalStateException(String msg, Throwable t) {
        super(StatusCode.ILLEGAL_STATE, msg, t);
    }
}
