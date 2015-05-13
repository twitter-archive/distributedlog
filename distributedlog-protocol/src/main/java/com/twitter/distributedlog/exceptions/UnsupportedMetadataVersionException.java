package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

public class UnsupportedMetadataVersionException extends DLException {

    private static final long serialVersionUID = 4980892659955478446L;

    public UnsupportedMetadataVersionException(String message) {
        super(StatusCode.UNSUPPORTED_METADATA_VERSION, String.format(message));
    }
}
