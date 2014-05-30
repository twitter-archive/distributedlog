package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

public class StreamUnavailableException extends DLException {

    private static final long serialVersionUID = 684211282036993028L;

    public StreamUnavailableException(String msg) {
        super(StatusCode.STREAM_UNAVAILABLE, msg);
    }
}
