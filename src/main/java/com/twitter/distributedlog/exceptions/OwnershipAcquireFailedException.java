package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

public class OwnershipAcquireFailedException extends DLException {
    private static final long serialVersionUID = 1L;
    private final String currentOwner;

    public OwnershipAcquireFailedException(String message, String currentOwner) {
        super(StatusCode.FOUND, message);
        this.currentOwner = currentOwner;
    }

    public String getCurrentOwner() {
        return currentOwner;
    }
}
