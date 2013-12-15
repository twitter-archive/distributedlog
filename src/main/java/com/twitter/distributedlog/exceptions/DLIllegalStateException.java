package com.twitter.distributedlog.exceptions;

import java.io.IOException;

public class DLIllegalStateException extends IOException {

    public DLIllegalStateException(String msg) {
        super(msg);
    }

    public DLIllegalStateException(String msg, Throwable t) {
        super(msg, t);
    }
}
