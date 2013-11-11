package com.twitter.distributedlog.exceptions;

import java.io.IOException;

public class NotYetImplementedException extends IOException {
    public NotYetImplementedException(String method) {
        super(method + "is not supported by the current version");
    }
}
