package com.twitter.distributedlog;

import com.twitter.distributedlog.exceptions.DLException;
import com.twitter.distributedlog.thrift.service.StatusCode;

public class LogNotFoundException extends DLException {

    private static final long serialVersionUID = 871435700699403164L;

    public LogNotFoundException(String message) {
        super(StatusCode.LOG_NOT_FOUND, message);
    }
}
