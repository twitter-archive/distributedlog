package com.twitter.distributedlog.exceptions;

import java.io.IOException;

public class OwnershipAcquireFailedException extends IOException {
    private static final long serialVersionUID = 1L;
    private final String currentOwner;

    public OwnershipAcquireFailedException(String lockPath, String currentOwner) {
        super(String.format("Lock acquisition failed for %s, the current owner is %s", lockPath, currentOwner));
        this.currentOwner = currentOwner;
    }

    public String getCurrentOwner() {
        return currentOwner;
    }
}
