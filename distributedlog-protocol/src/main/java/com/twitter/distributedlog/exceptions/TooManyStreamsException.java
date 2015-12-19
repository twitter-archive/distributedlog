package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

public class TooManyStreamsException extends OverCapacityException {

    private static final long serialVersionUID = -6391941401860180163L;

    public TooManyStreamsException(String message) {
        super(StatusCode.TOO_MANY_STREAMS, message);
    }
}
