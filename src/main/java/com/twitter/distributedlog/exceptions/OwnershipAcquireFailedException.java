package com.twitter.distributedlog.exceptions;

import java.io.IOException;

public class OwnershipAcquireFailedException extends IOException {
    private static final long serialVersionUID = 1L;
    private final String currentOwner;

    public OwnershipAcquireFailedException(String message, String currentOwner) {
        super(message);
        this.currentOwner = currentOwner;
    }

    public String getCurrentOwner() {
        return currentOwner;
    }
}
