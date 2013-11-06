package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

public class OwnershipAcquireFailedException extends DLException {
    private static final long serialVersionUID = 1L;
    private final String currentOwner;

    public OwnershipAcquireFailedException(String lockPath, String currentOwner) {
        super(StatusCode.FOUND,
              String.format("Lock acquisition failed for %s, the current owner is %s", lockPath, currentOwner));
        this.currentOwner = currentOwner;
    }

    public String getCurrentOwner() {
        return currentOwner;
    }
}
