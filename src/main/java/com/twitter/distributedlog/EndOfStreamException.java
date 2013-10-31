package com.twitter.distributedlog;

import java.io.IOException;

public class EndOfStreamException extends IOException {
    private static final long serialVersionUID = 1L;

    public EndOfStreamException(String message) {
        super(message);
    }
}