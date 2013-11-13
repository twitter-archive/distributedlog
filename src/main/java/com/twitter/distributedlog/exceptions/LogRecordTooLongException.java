package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

public class LogRecordTooLongException extends DLException {
    private static final long serialVersionUID = 1L;

    public LogRecordTooLongException(String message) {
        super(StatusCode.TOO_LARGE_RECORD, message);
    }
}
