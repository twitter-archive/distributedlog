package com.twitter.distributedlog.v2.exceptions;

import java.io.IOException;

public class OverCapacityException extends IOException {

    private static final long serialVersionUID = -6398949404860680263L;

    public OverCapacityException(String message) {
        super(message);
    }
}
