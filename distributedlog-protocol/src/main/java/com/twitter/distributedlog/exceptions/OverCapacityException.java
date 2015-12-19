package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

public class OverCapacityException extends DLException {

    private static final long serialVersionUID = -6398949404860680263L;

    public OverCapacityException(String message) {
        super(StatusCode.OVER_CAPACITY, message);
    }

    public OverCapacityException(StatusCode code, String message) {
        super(code, message);
    }
}
