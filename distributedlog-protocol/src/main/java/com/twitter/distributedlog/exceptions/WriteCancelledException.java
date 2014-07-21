package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

public class WriteCancelledException extends DLException {

    private static final long serialVersionUID = -1836146493496072122L;

    public WriteCancelledException(String stream) {
        super(StatusCode.WRITE_CANCELLED_EXCEPTION,
            "Write cancelled due to earlier error on stream " + 
            stream);
    }
}
