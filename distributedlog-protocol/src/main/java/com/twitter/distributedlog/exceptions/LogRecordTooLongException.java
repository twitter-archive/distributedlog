package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

public class LogRecordTooLongException extends DLException {

    private static final long serialVersionUID = 2788274084603111386L;

    public LogRecordTooLongException(String message) {
        super(StatusCode.TOO_LARGE_RECORD, message);
    }
}
