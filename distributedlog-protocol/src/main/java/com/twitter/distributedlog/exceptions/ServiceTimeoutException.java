package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

public class ServiceTimeoutException extends DLException {
    private static final long serialVersionUID = 8176051926552748001L;

    public ServiceTimeoutException() {
        super(StatusCode.SERVICE_TIMEOUT,
              String.format("Stream operation timed out in the proxy service"));
    }
}
