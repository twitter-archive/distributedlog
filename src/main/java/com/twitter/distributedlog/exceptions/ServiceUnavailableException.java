package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

public class ServiceUnavailableException extends DLException {

    private static final long serialVersionUID = 6317900286881665746L;

    public ServiceUnavailableException(String msg) {
        super(StatusCode.SERVICE_UNAVAILABLE, msg);
    }
}
