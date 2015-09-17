package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

/**
 * Log Already Exists
 */
public class LogExistsException extends DLException {
    private static final long serialVersionUID = 1794053581673506784L;

    public LogExistsException(String msg) {
        super(StatusCode.LOG_EXISTS, msg);
    }
}
