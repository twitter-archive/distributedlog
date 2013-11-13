package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

public class ServiceUnavailableException extends DLException {
    public ServiceUnavailableException(String msg) {
        super(StatusCode.SERVICE_UNAVAILABLE, msg);
    }
}
