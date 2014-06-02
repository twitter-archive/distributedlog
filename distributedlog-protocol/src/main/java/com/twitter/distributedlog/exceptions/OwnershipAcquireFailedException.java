package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.LockingException;
import com.twitter.distributedlog.thrift.service.StatusCode;

public class OwnershipAcquireFailedException extends LockingException {
    private static final long serialVersionUID = 8176056926552748001L;
    private final String currentOwner;

    public OwnershipAcquireFailedException(String lockPath, String currentOwner) {
        super(StatusCode.FOUND, lockPath,
              String.format("Lock acquisition failed, the current owner is %s", currentOwner));
        this.currentOwner = currentOwner;
    }

    public String getCurrentOwner() {
        return currentOwner;
    }
}
