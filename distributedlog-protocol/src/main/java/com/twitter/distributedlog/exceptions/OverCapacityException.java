package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

public class OverCapacityException extends DLException {

    private static final long serialVersionUID = -6398949404860680263L;

    public OverCapacityException(int outstanding, int maxOutstanding) {
        super(StatusCode.OVER_CAPACITY, String.format(
            "Too many outstanding writes (outstanding=%d, limit=%d)", outstanding, maxOutstanding));
    }
}
