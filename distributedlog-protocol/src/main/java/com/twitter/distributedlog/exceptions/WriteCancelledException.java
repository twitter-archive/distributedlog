package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

public class WriteCancelledException extends DLException {

    private static final long serialVersionUID = -1836146493496072122L;

    public WriteCancelledException(String stream, Throwable t) {
        super(StatusCode.WRITE_CANCELLED_EXCEPTION,
            "Write cancelled on stream " +
            stream + " due to an earlier error", t);
    }

    public WriteCancelledException(String stream) {
        super(StatusCode.WRITE_CANCELLED_EXCEPTION,
            "Write cancelled on stream " +
            stream + " due to an earlier error");
    }
}
