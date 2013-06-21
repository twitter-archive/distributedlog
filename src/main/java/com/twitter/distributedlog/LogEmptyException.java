package com.twitter.distributedlog;

import java.io.IOException;

public class LogEmptyException extends IOException {

    private static final long serialVersionUID = 1L;

    public LogEmptyException(String message) {
        super(message);
    }
}
