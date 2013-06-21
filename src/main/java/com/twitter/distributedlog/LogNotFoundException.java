package com.twitter.distributedlog;

import java.io.IOException;

public class LogNotFoundException extends IOException {

    private static final long serialVersionUID = 1L;

    public LogNotFoundException(String message) {
        super(message);
    }
}
