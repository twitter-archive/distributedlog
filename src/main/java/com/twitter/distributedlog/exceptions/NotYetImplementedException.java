package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

public class NotYetImplementedException extends DLException {
    public NotYetImplementedException(String method) {
        super(StatusCode.NOT_IMPLEMENTED, method + "is not supported by the current version");
    }
}
