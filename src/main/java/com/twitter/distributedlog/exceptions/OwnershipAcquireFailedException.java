package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.LockingException;

public class OwnershipAcquireFailedException extends LockingException {
    private static final long serialVersionUID = 1L;
    private final String currentOwner;

    public OwnershipAcquireFailedException(String lockPath, String currentOwner) {
        super(lockPath, String.format("Lock acquisition failed, the current owner is %s", currentOwner));
        this.currentOwner = currentOwner;
    }

    public String getCurrentOwner() {
        return currentOwner;
    }
}
