package com.twitter.distributedlog.exceptions;

import java.io.IOException;

public class LogRecordTooLongException extends IOException {
    private static final long serialVersionUID = 1L;

    public LogRecordTooLongException(String message) {
        super(message);
    }
}
