package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

public class ChecksumFailedException extends DLException {

    private static final long serialVersionUID = 288438128880378812L;

    public ChecksumFailedException() {
        super(StatusCode.CHECKSUM_FAILED, "Checksum failed");
    }
}
