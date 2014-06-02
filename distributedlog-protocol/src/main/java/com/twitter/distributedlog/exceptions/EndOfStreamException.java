package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

public class EndOfStreamException extends DLException {

    private static final long serialVersionUID = -6398949401860680263L;

    public EndOfStreamException(String message) {
        super(StatusCode.END_OF_STREAM, message);
    }
}
