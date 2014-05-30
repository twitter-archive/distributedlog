package com.twitter.distributedlog;

import com.twitter.distributedlog.exceptions.DLException;
import com.twitter.distributedlog.thrift.service.StatusCode;

public class LogEmptyException extends DLException {

    private static final long serialVersionUID = -1106184127178002282L;

    public LogEmptyException(String message) {
        super(StatusCode.LOG_EMPTY, message);
    }
}
