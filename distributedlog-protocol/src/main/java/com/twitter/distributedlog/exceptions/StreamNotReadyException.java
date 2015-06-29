package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

public class StreamNotReadyException extends DLException {

    private static final long serialVersionUID = 684211282036293028L;

    public StreamNotReadyException(String msg) {
        super(StatusCode.STREAM_NOT_READY, msg);
    }
}
