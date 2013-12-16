package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

public class MetadataException extends DLException {

    public MetadataException(String msg) {
        super(StatusCode.METADATA_EXCEPTION, msg);
    }

    public MetadataException(String msg, Throwable t) {
        super(StatusCode.METADATA_EXCEPTION, msg, t);
    }
}
