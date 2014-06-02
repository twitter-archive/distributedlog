package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

public class NotYetImplementedException extends DLException {

    private static final long serialVersionUID = -6002036746792556106L;

    public NotYetImplementedException(String method) {
        super(StatusCode.NOT_IMPLEMENTED, method + "is not supported by the current version");
    }
}
